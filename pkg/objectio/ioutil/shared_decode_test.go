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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// Keep the first completed column read alive until the second read has its
// own result. This exercises the public Top-N consumer's actual scope without
// relying on scheduler timing or changing its production ownership.
type coordinatedTopNFS struct {
	fileservice.FileService
	disableSharing bool
	first          chan fileservice.IOVector
	both           chan struct{}
	mu             sync.Mutex
	owners         []fscache.Data
	counters       *perfcounter.CounterSet
	diskHits       []int64
	s3Reads        []int64
}

func (f *coordinatedTopNFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	selected := len(v.Entries) == 1 && v.Entries[0].DecodeSharing.Codec != ""
	if selected && f.disableSharing {
		v.Entries[0].DecodeSharing = fileservice.DecodeSharing{}
	}
	var beforeDisk, beforeS3 int64
	if selected && f.counters != nil {
		beforeDisk = f.counters.FileService.Cache.Disk.Hit.Load()
		beforeS3 = f.counters.FileService.S3.Get.Load()
	}
	if err := f.FileService.Read(ctx, v); err != nil || !selected {
		return err
	}
	f.mu.Lock()
	f.owners = append(f.owners, v.Entries[0].CachedData)
	if f.counters != nil {
		f.diskHits = append(f.diskHits, f.counters.FileService.Cache.Disk.Hit.Load()-beforeDisk)
		f.s3Reads = append(f.s3Reads, f.counters.FileService.S3.Get.Load()-beforeS3)
	}
	if len(f.owners) == 1 {
		f.first <- fileservice.IOVector{FilePath: v.FilePath, Entries: []fileservice.IOEntry{{
			Offset: v.Entries[0].Offset, Size: v.Entries[0].Size,
		}}}
	} else {
		close(f.both)
	}
	f.mu.Unlock()
	select {
	case <-f.both:
		return nil
	case <-ctx.Done():
		v.ReleaseReadResultOnError()
		return ctx.Err()
	}
}

func TestLoadColumnDataByTopNScopedSharing(t *testing.T) {
	ctx := context.Background()
	capacity, diskCapacity := toml.ByteSize(4<<10), toml.ByteSize(1<<20)
	diskPath := t.TempDir()
	var counters perfcounter.CounterSet
	fs, err := fileservice.NewS3FS(ctx,
		fileservice.ObjectStorageArguments{Name: "topn-sharing", Endpoint: "disk", Bucket: t.TempDir()},
		fileservice.CacheConfig{MemoryCapacity: &capacity, DiskCapacity: &diskCapacity, DiskPath: &diskPath},
		[]*perfcounter.CounterSet{&counters}, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_array_float32.ToType())
	for _, value := range []float32{1, 2, 3, 4} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], types.ArrayToBytes([]float32{value}), false, mp))
	}
	bat.SetRowCount(4)
	writer := ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
	mpool.DeleteMPool(mp)
	require.NoError(t, fs.PrefetchFile(ctx, location.Name().String()))
	fs.FlushCache(ctx)
	fileservice.SetMemoryCachePressureTargetPercentByOwner(t.Name(), 1, time.Now().Add(10*time.Minute))
	t.Cleanup(func() { fileservice.ClearMemoryCachePressureTargetByOwner(t.Name()) })

	type query struct {
		value     float32
		selection []int64
		bound     plan.BoundType
		upper     float64
		rows      []int64
		distances []float64
	}
	pairs := [][2]query{
		{{value: 0, rows: []int64{0}, distances: []float64{1}},
			{value: 5, selection: []int64{1, 2}, rows: []int64{2}, distances: []float64{4}}},
		{{value: 5, selection: []int64{1, 2}, bound: plan.BoundType_EXCLUSIVE, upper: 4, rows: []int64{}, distances: []float64{}},
			{value: 0, selection: []int64{}, rows: []int64{}, distances: []float64{}}},
	}
	for _, share := range []bool{false, true} {
		for _, pair := range pairs {
			fs.FlushCache(ctx)
			wrapped := &coordinatedTopNFS{FileService: fs, disableSharing: !share,
				first: make(chan fileservice.IOVector, 1), both: make(chan struct{}), counters: &counters}
			readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			var workers sync.WaitGroup
			t.Cleanup(func() { cancel(); workers.Wait() })
			type result struct {
				index     int
				rows      []int64
				dists     []float64
				err       error
				remaining int64
			}
			results := make(chan result, 2)
			start := func(index int) {
				q := pair[index]
				workers.Add(1)
				go func() {
					defer workers.Done()
					m := mpool.MustNewZero()
					top := &objectio.IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2Distance,
						NumVec: types.ArrayToBytes([]float32{q.value}), Limit: 1, UpperBoundType: q.bound, UpperBound: q.upper}
					rows, dists, _, err := LoadColumnDataByTopN(readCtx, 0, types.T_array_float32.ToType(),
						wrapped, location, q.selection, top, m, fileservice.SkipFullFilePreloads)
					remaining := m.CurrNB()
					mpool.DeleteMPool(m)
					results <- result{index, rows, dists, err, remaining}
				}()
			}
			start(0)
			var probe fileservice.IOVector
			select {
			case probe = <-wrapped.first:
			case <-readCtx.Done():
				t.Fatal("first Top-N column read did not complete")
			}
			require.NoError(t, fs.ReadCache(readCtx, &probe))
			cached := probe.Entries[0].CachedData != nil
			probe.Release()
			require.False(t, cached, "the first decoded column must be rejected by the memory cache")
			start(1)
			got := []result{<-results, <-results}
			workers.Wait()
			cancel()
			for _, r := range got {
				require.NoError(t, r.err)
				require.Zero(t, r.remaining)
				require.EqualValues(t, pair[r.index].rows, r.rows)
				require.EqualValues(t, pair[r.index].distances, r.dists)
			}
			require.Equal(t, []int64{1, 1}, wrapped.diskHits)
			require.Equal(t, []int64{0, 0}, wrapped.s3Reads)
			if share {
				require.Same(t, wrapped.owners[0], wrapped.owners[1])
			} else {
				require.NotSame(t, wrapped.owners[0], wrapped.owners[1])
			}
		}
	}
}
