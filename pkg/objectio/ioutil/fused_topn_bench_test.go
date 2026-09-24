//go:build linux || darwin

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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

// Keep this fixture compatible with the base revision: it benchmarks the
// unchanged public fused loader, including filters and projected winner rows.
type fusedBenchFS struct {
	fileservice.FileService
	decoded, maximum atomic.Int64
}

func (f *fusedBenchFS) track(v *fileservice.IOVector) {
	for i := range v.Entries {
		original := v.Entries[i].ToCacheData
		if original == nil {
			continue
		}
		v.Entries[i].ToCacheData = func(ctx context.Context, r io.Reader, data []byte, a fileservice.CacheDataAllocator) (fscache.Data, error) {
			out, err := original(ctx, r, data, a)
			if out != nil && out.Capacity() > 1<<20 {
				f.decoded.Add(out.Capacity())
				for old := f.maximum.Load(); old < out.Capacity(); old = f.maximum.Load() {
					if f.maximum.CompareAndSwap(old, out.Capacity()) {
						break
					}
				}
			}
			return out, err
		}
	}
}

func (f *fusedBenchFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	f.track(v)
	return f.FileService.Read(ctx, v)
}

func (f *fusedBenchFS) DecodeFromBytes(ctx context.Context, v *fileservice.IOVector, data []byte) error {
	f.track(v)
	return f.FileService.(interface {
		DecodeFromBytes(context.Context, *fileservice.IOVector, []byte) error
	}).DecodeFromBytes(ctx, v, data)
}

func BenchmarkFusedTopN(b *testing.B) {
	for _, mode := range []string{"dense", "sparse", "overlap", "legacy", "hot_whole", "hot_self"} {
		b.Run(mode, func(b *testing.B) {
			ctx := context.Background()
			capacity := toml.ByteSize(64 << 20)
			var counters perfcounter.CounterSet
			ctx = perfcounter.WithCounterSet(ctx, &counters)
			storage, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{Name: "fused-bench", Endpoint: "disk", Bucket: b.TempDir()},
				fileservice.CacheConfig{MemoryCapacity: &capacity}, []*perfcounter.CounterSet{&counters}, false, false)
			require.NoError(b, err)
			b.Cleanup(func() { storage.Close(ctx) })
			pool := mpool.MustNewZero()
			b.Cleanup(func() { require.Zero(b, pool.CurrNB()); mpool.DeleteMPool(pool) })
			const rows, dimensions = 4096, 768
			typs := []types.Type{types.T_int64.ToType(), types.T_array_float32.ToType(), types.T_int64.ToType()}
			input := batch.NewWithSize(3)
			for i := range typs {
				input.Vecs[i] = vector.NewVec(typs[i])
			}
			b.Cleanup(func() { input.Clean(pool) })
			values := make([]float32, dimensions)
			for row := range rows {
				values[0] = float32(row)
				for dim := 1; dim < dimensions; dim++ {
					values[dim] = float32(dim%113) / 113
				}
				require.NoError(b, vector.AppendFixed(input.Vecs[0], int64(row), false, pool))
				require.NoError(b, vector.AppendArray(input.Vecs[1], values, false, pool))
				require.NoError(b, vector.AppendFixed(input.Vecs[2], int64(row+10), false, pool))
			}
			input.SetRowCount(rows)
			id := objectio.NewObjectid()
			name := objectio.BuildObjectNameWithObjectID(&id)
			writer, err := objectio.NewObjectWriter(name, storage, 0, []uint16{0, 1, 2}, nil)
			require.NoError(b, err)
			writer.SetChunkedColumnPolicy(func() bool { return mode != "legacy" })
			_, err = writer.Write(input)
			require.NoError(b, err)
			blocks, err := writer.WriteEnd(ctx)
			require.NoError(b, err)
			location := objectio.BuildLocation(name, blocks[0].GetExtent(), rows, 0)
			meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, storage)
			require.NoError(b, err)
			storage.FlushCache(ctx)
			if mode != "hot_whole" && mode != "hot_self" {
				pin := storage.AllocateCacheData(ctx, int(capacity))
				b.Cleanup(pin.Release)
			}
			if mode == "hot_whole" {
				dataMeta := meta.MustGetMeta(objectio.SchemaData)
				v, err := objectio.ReadOneBlock(ctx, &dataMeta, name.String(), 0, []uint16{1}, []types.Type{typs[1]}, pool, storage, fileservice.SkipFullFilePreloads)
				require.NoError(b, err)
				v.Release()
			}
			fs := &fusedBenchFS{FileService: storage}
			selected := make([]int64, 0, rows)
			for row := range rows {
				if mode != "sparse" || row >= 3072 && row%32 == 0 {
					selected = append(selected, int64(row))
				}
			}
			query := types.ArrayToBytes(make([]float32, dimensions))
			runOne := func(ready *sync.WaitGroup, start <-chan struct{}) error {
				signal := func() {}
				if ready != nil {
					signal = sync.OnceFunc(ready.Done)
				}
				defer signal() // read failures must not strand the cohort barrier
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				filter, output := vector.NewVec(typs[0]), vector.NewVec(typs[2])
				defer filter.Free(mp)
				defer output.Free(mp)
				op := &objectio.IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2Distance, NumVec: query, Limit: 10}
				got, _, _, err := LoadColumnsDataIntoAndTopN(ctx, []uint16{0}, []types.Type{typs[0]}, fs, location,
					[]*vector.Vector{filter}, nil, 1, typs[1], []uint16{2}, []types.Type{typs[2]}, []*vector.Vector{output},
					func() ([]int64, error) {
						if ready != nil {
							signal()
							<-start
						}
						return selected, nil
					}, op, mp, fileservice.SkipFullFilePreloads)
				if err == nil && (len(got) != 10 || output.Length() != 10) {
					return fmt.Errorf("unexpected winners: %v", got)
				}
				return err
			}
			run := func() {
				if mode != "overlap" {
					require.NoError(b, runOne(nil, nil))
					return
				}
				var ready sync.WaitGroup
				ready.Add(8)
				start := make(chan struct{})
				done := make(chan error, 8)
				for range 8 {
					go func() { done <- runOne(&ready, start) }()
				}
				ready.Wait()
				close(start)
				var firstErr error
				for range 8 {
					if err := <-done; err != nil && firstErr == nil {
						firstErr = err
					}
				}
				require.NoError(b, firstErr)
			}
			run()
			fs.decoded.Store(0)
			fs.maximum.Store(0)
			gets, readBytes := counters.FileService.S3.Get.Load(), counters.FileService.S3ReadSize.Load()
			var before, after syscall.Rusage
			require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &before))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
			require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &after))
			b.ReportMetric(float64(after.Utime.Nano()+after.Stime.Nano()-before.Utime.Nano()-before.Stime.Nano())/float64(b.N), "cpu-ns/op")
			b.ReportMetric(float64(fs.decoded.Load())/float64(b.N), "decoded-B/op")
			b.ReportMetric(float64(fs.maximum.Load()), "max-decoded-B")
			b.ReportMetric(float64(counters.FileService.S3.Get.Load()-gets)/float64(b.N), "gets/op")
			b.ReportMetric(float64(counters.FileService.S3ReadSize.Load()-readBytes)/float64(b.N), "read-B/op")
		})
	}
}
