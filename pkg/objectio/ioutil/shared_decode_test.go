// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ioutil

import (
	"context"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

// Observe, but never wrap, the concrete validated cache data. Consumers must
// retain its immutable-view contract as well as share its physical ownership.
type fusedDecodeFS struct {
	fileservice.FileService
	t       *testing.T
	decoded int
	owners  []fscache.Data
}

func (f *fusedDecodeFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	selected := -1
	for i := range v.Entries {
		if v.Entries[i].DecodeSharing.Codec == "" {
			continue
		}
		require.Equal(f.t, -1, selected, "only the Top-K column may be marked")
		require.Equal(f.t, 1, i, "the synthetic predecessor must not shift the physical target")
		require.Len(f.t, v.Entries, 3, "filter, target and deferred column stay in one read")
		selected = i
		original := v.Entries[i].ToCacheData
		v.Entries[i].ToCacheData = func(ctx context.Context, r io.Reader, data []byte, a fileservice.CacheDataAllocator) (fscache.Data, error) {
			f.decoded++
			return original(ctx, r, data, a)
		}
	}
	if err := f.FileService.Read(ctx, v); err != nil {
		return err
	}
	if selected >= 0 {
		f.owners = append(f.owners, v.Entries[selected].CachedData)
	}
	return nil
}

func TestFusedTopNSelectedDecodeIsolation(t *testing.T) {
	ctx := t.Context()
	capacity := toml.ByteSize(128 << 10)
	storage, err := fileservice.NewS3FS(ctx,
		fileservice.ObjectStorageArguments{Name: "fused-decode", Endpoint: "disk", Bucket: t.TempDir()},
		fileservice.CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { storage.Close(context.Background()) })
	fs := &fusedDecodeFS{FileService: storage, t: t}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(3)
	typs := []types.Type{types.T_int64.ToType(), types.T_array_float32.ToType(), types.T_array_float32.ToType()}
	for i := range typs {
		bat.Vecs[i] = vector.NewVec(typs[i])
	}
	t.Cleanup(func() { bat.Clean(mp) })
	for row := range 4 {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], types.ArrayToBytes([]float32{float32(row + 1)}), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], types.ArrayToBytes([]float32{float32(row + 10)}), false, mp))
	}
	bat.SetRowCount(4)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0, 1, 2}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	location := objectio.BuildLocation(name, blocks[0].GetExtent(), 4, 0)
	_, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	storage.FlushCache(ctx)
	pinned := storage.AllocateCacheData(ctx, int(capacity))
	t.Cleanup(pinned.Release)

	call := func(t *testing.T, query float32, bound plan.BoundType, materialize []int64, selectRows func(*vector.Vector) ([]int64, error)) ([]int64, []float64, []float32, error) {
		pool := mpool.MustNewZero()
		defer func() { require.Zero(t, pool.CurrNB()); mpool.DeleteMPool(pool) }()
		missing, filter, projection := vector.NewVec(typs[0]), vector.NewVec(typs[0]), vector.NewVec(typs[2])
		defer missing.Free(pool)
		defer filter.Free(pool)
		defer projection.Free(pool)
		op := &objectio.IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2Distance,
			NumVec: types.ArrayToBytes([]float32{query}), Limit: 1, UpperBoundType: bound, UpperBound: 4}
		rows, distances, _, err := LoadColumnsDataIntoAndTopN(ctx,
			[]uint16{3, 0}, []types.Type{typs[0], typs[0]}, fs, location, []*vector.Vector{missing, filter}, materialize,
			1, typs[1], []uint16{2}, []types.Type{typs[2]}, []*vector.Vector{projection},
			func() ([]int64, error) { return selectRows(filter) }, op, pool, fileservice.SkipFullFilePreloads)
		var projected []float32
		for i := 0; i < projection.Length(); i++ {
			projected = append(projected, types.BytesToArray[float32](projection.GetBytesAt(i))...)
		}
		return rows, distances, projected, err
	}
	all := func(*vector.Vector) ([]int64, error) { return []int64{0, 1, 2, 3}, nil }
	rows, distances, projected, err := call(t, 0, plan.BoundType_UNBOUNDED, nil, func(filter *vector.Vector) ([]int64, error) {
		require.Equal(t, []int64{0, 1, 2, 3}, vector.MustFixedColNoTypeCheck[int64](filter))
		owner := fs.owners[0]
		snapshot := owner.Bytes()
		// Reentrant independent consumers overlap the outer selector's scoped
		// lifetime deterministically, with no scheduler or wall-clock dependency.
		t.Run("different filter query and projection", func(t *testing.T) {
			r, d, p, err := call(t, 5, plan.BoundType_UNBOUNDED, nil, func(v *vector.Vector) ([]int64, error) {
				require.Equal(t, []int64{0, 1, 2, 3}, vector.MustFixedColNoTypeCheck[int64](v))
				return []int64{1, 2}, nil
			})
			require.NoError(t, err)
			require.Equal(t, []int64{2}, r)
			require.Equal(t, []float64{4}, d)
			require.Equal(t, []float32{12}, p)
		})
		t.Run("independent threshold", func(t *testing.T) {
			r, _, p, err := call(t, 5, plan.BoundType_EXCLUSIVE, nil, func(*vector.Vector) ([]int64, error) { return []int64{1, 2}, nil })
			require.NoError(t, err)
			require.Empty(t, r)
			require.Empty(t, p)
		})
		t.Run("selector error", func(t *testing.T) {
			_, _, _, err := call(t, 0, plan.BoundType_UNBOUNDED, nil, func(*vector.Vector) ([]int64, error) {
				return nil, moerr.NewInternalErrorNoCtx("injected selector failure")
			})
			require.ErrorContains(t, err, "selector failure")
		})
		t.Run("materialization error", func(t *testing.T) {
			_, _, _, err := call(t, 0, plan.BoundType_UNBOUNDED, []int64{99}, all)
			require.ErrorContains(t, err, "out of range")
		})
		for _, data := range fs.owners {
			require.Same(t, owner, data)
		}
		require.Equal(t, snapshot, owner.Bytes())
		require.Equal(t, 1, fs.decoded)
		return all(filter)
	})
	require.NoError(t, err)
	require.Equal(t, []int64{0}, rows)
	require.Equal(t, []float64{1}, distances)
	require.Equal(t, []float32{10}, projected)
	_, _, _, err = call(t, 0, plan.BoundType_UNBOUNDED, nil, all)
	require.NoError(t, err)
	require.Equal(t, 2, fs.decoded, "every success/error scope must release its ticket before the next generation")
	require.NotSame(t, fs.owners[0], fs.owners[len(fs.owners)-1])
}
