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

package objectio

import (
	"context"
	"io"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type topNBenchFS struct {
	fileservice.FileService
	ranges, bytes, decoded, maxDecoded int64
}

func (f *topNBenchFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	for i := range v.Entries {
		e := &v.Entries[i]
		f.ranges++
		f.bytes += e.Size
		original := e.ToCacheData
		if original != nil {
			e.ToCacheData = func(ctx context.Context, r io.Reader, data []byte, a fileservice.CacheDataAllocator) (fscache.Data, error) {
				out, err := original(ctx, r, data, a)
				if out != nil {
					f.decoded += out.Capacity()
					f.maxDecoded = max(f.maxDecoded, out.Capacity())
				}
				return out, err
			}
		}
	}
	return f.FileService.Read(ctx, v)
}

func benchmarkReadWholeTopN(ctx context.Context, typ types.Type, fs fileservice.FileService, location Location, selected []int64, top *IndexReaderTopOp, mp *mpool.MPool) ([]int64, []float64, bool, error) {
	meta, err := FastLoadObjectMeta(ctx, &location, false, fs)
	if err != nil {
		return nil, nil, false, err
	}
	dataMeta := meta.MustGetMeta(SchemaData)
	ioVec, err := ReadOneBlock(ctx, &dataMeta, location.Name().UnsafeString(), location.ID(), []uint16{0}, []types.Type{typ}, mp, fs, 0, ShareScopedDecodedColumn)
	if err != nil {
		return nil, nil, false, err
	}
	defer ioVec.Release()
	var source vector.Vector
	if err = bindCachedVectorForScope(&source, ioVec.Entries[0].CachedData); err != nil {
		return nil, nil, false, err
	}
	defer source.Free(nil)
	rows, distances, err := TopNVector(ctx, selected, &source, top)
	return rows, distances, ioVec.Entries[0].WasFromCache(), err
}

func BenchmarkReadColumnTopN(b *testing.B) {
	for _, mode := range []string{"hot_cache", "legacy", "chunk_pressure", "chunk_sparse", "cold_ranges"} {
		b.Run(mode, func(b *testing.B) {
			for _, stream := range []bool{false, true} {
				name := "whole"
				if stream {
					name = "stream"
				}
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()
					mp := newTopNTestMP(b)
					source := vector.NewVec(types.T_array_float32.ToType())
					b.Cleanup(func() { source.Free(mp) })
					const rows, dimensions = 4096, 768
					values := make([]float32, dimensions)
					for row := 0; row < rows; row++ {
						values[0] = float32(rows - row)
						for dim := 1; dim < dimensions; dim++ {
							values[dim] = float32((dim*19)%113) / 113
						}
						require.NoError(b, vector.AppendArray(source, values, false, mp))
					}
					capacity := toml.ByteSize(8 << 20)
					if mode == "hot_cache" {
						capacity = 64 << 20
					}
					cache := fileservice.CacheConfig{MemoryCapacity: &capacity}
					if mode != "cold_ranges" {
						disk := b.TempDir()
						diskCapacity := toml.ByteSize(64 << 20)
						cache.DiskPath = &disk
						cache.DiskCapacity = &diskCapacity
					}
					fs, err := fileservice.NewS3FS(ctx, fileservice.ObjectStorageArguments{Name: "topn-bench", Endpoint: "disk", Bucket: b.TempDir()}, cache, nil, false, false)
					require.NoError(b, err)
					b.Cleanup(func() { fs.Close(ctx) })
					var payload []byte
					if mode != "legacy" {
						payload, _ = encodeTopNTestChunks(b, source, 1024, mp)
					}
					location, _, _ := persistTopNTestColumn(b, fs, source, payload)
					if mode != "cold_ranges" {
						require.NoError(b, fs.PrefetchFile(ctx, location.Name().UnsafeString()))
					}
					fs.FlushCache(ctx)
					if mode != "hot_cache" {
						pinned := fs.AllocateCacheData(ctx, int(capacity))
						b.Cleanup(pinned.Release)
					}
					query := make([]float32, dimensions)
					var selected []int64
					if mode == "chunk_sparse" {
						for row := 3072; row < rows; row += 16 {
							selected = append(selected, int64(row))
						}
					}
					newOp := func() *IndexReaderTopOp { op := newTopNTestOp(10); op.NumVec = types.ArrayToBytes(query); return op }
					if mode == "hot_cache" {
						_, _, _, err := benchmarkReadWholeTopN(ctx, *source.GetType(), fs, location, selected, newOp(), mp)
						require.NoError(b, err)
					}
					tracked := &topNBenchFS{FileService: fs}
					run := func() error {
						var got []int64
						var err error
						if stream {
							got, _, _, err = ReadColumnTopN(ctx, 0, *source.GetType(), tracked, location, selected, newOp(), mp, 0)
						} else {
							got, _, _, err = benchmarkReadWholeTopN(ctx, *source.GetType(), tracked, location, selected, newOp(), mp)
						}
						if err != nil {
							return err
						}
						if len(got) != 10 {
							b.Fatalf("expected 10 winners, got %d", len(got))
						}
						return nil
					}
					require.NoError(b, run())
					tracked.ranges, tracked.bytes, tracked.decoded, tracked.maxDecoded = 0, 0, 0, 0
					var before, after syscall.Rusage
					require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &before))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if err := run(); err != nil {
							b.Fatal(err)
						}
					}
					require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &after))
					b.ReportMetric(float64(after.Utime.Nano()+after.Stime.Nano()-before.Utime.Nano()-before.Stime.Nano())/float64(b.N), "cpu-ns/op")
					b.ReportMetric(float64(tracked.ranges)/float64(b.N), "read-calls/op")
					b.ReportMetric(float64(tracked.bytes)/float64(b.N), "requested-B/op")
					b.ReportMetric(float64(tracked.decoded)/float64(b.N), "decoded-B/op")
					b.ReportMetric(float64(tracked.maxDecoded), "max-decoded-B")
				})
			}
		})
	}
}
