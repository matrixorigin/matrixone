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

package fileservice

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

// The control omits only the opt-in descriptor: I/O, conversion, cache policy,
// and payload are identical. Independent reads have no overlapping lifetimes;
// overlapping reads hold their results until every participant has completed.
func BenchmarkS3FSSharedDecode(b *testing.B) {
	for _, mode := range []string{"memory_hit", "independent", "independent_keys", "overlapping", "warm_fill"} {
		b.Run(mode, func(b *testing.B) {
			for _, share := range []bool{false, true} {
				name := "off"
				if share {
					name = "on"
				}
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()
					var counters perfcounter.CounterSet
					fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "decode-bench", Endpoint: "disk", Bucket: b.TempDir()},
						CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](1 << 20), DiskPath: ptrTo(b.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](2 << 20)}, []*perfcounter.CounterSet{&counters}, false, false)
					require.NoError(b, err)
					b.Cleanup(func() { fs.Close(ctx) })
					raw := bytes.Repeat([]byte("abcdef0123456789"), 32<<10)
					compressed := make([]byte, lz4.CompressBlockBound(len(raw)))
					n, err := lz4.CompressBlock(raw, compressed, nil)
					require.NoError(b, err)
					require.Positive(b, n)
					require.NoError(b, fs.Write(ctx, IOVector{FilePath: "column", Entries: []IOEntry{{Size: int64(n), Data: compressed[:n]}}, Policy: SkipAllCache}))
					require.NoError(b, fs.PrefetchFile(ctx, "column"))
					paths := []string{"column-a", "column-b", "column-c", "column-d", "column-e", "column-f", "column-g", "column-h"}
					if mode == "independent_keys" {
						for _, path := range paths {
							require.NoError(b, fs.Write(ctx, IOVector{FilePath: path, Entries: []IOEntry{{Size: int64(n), Data: compressed[:n]}}, Policy: SkipAllCache}))
							require.NoError(b, fs.PrefetchFile(ctx, path))
						}
					}
					fs.FlushCache(ctx)
					if mode != "memory_hit" && mode != "warm_fill" {
						owner := b.Name()
						SetMemoryCachePressureTargetPercentByOwner(owner, 1, time.Now().Add(time.Hour))
						b.Cleanup(func() { ClearMemoryCachePressureTargetByOwner(owner) })
					}
					var calls atomic.Int64
					convert := func(ctx context.Context, _ io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
						calls.Add(1)
						out := a.AllocateCacheData(ctx, len(raw))
						_, err := lz4.UncompressBlock(data, out.Bytes())
						if err != nil {
							out.Release()
							return nil, err
						}
						return out, nil
					}
					makeRead := func() IOVector {
						entry := IOEntry{Size: int64(n), CachedDataSize: int64(len(raw)), ToCacheData: convert}
						if share {
							entry.DecodeSharing = DecodeSharing{Codec: "benchmark-lz4-v1"}
						}
						return IOVector{FilePath: "column", Entries: []IOEntry{entry}}
					}
					warm := makeRead()
					require.NoError(b, fs.Read(ctx, &warm))
					warm.Release()
					if mode == "warm_fill" {
						fs.FlushCache(ctx)
					}
					if mode != "memory_hit" {
						require.Positive(b, calls.Load())
						probe := makeRead()
						require.NoError(b, fs.ReadCache(ctx, &probe))
						require.Nil(b, probe.Entries[0].CachedData, "benchmark must miss memory")
					}
					calls.Store(0)
					beforeDisk := counters.FileService.Cache.Disk.Read.Load()
					beforeMemory := counters.FileService.Cache.Memory.Hit.Load()
					beforeS3 := counters.FileService.S3.Get.Load()
					b.ReportAllocs()
					b.ResetTimer()
					if mode == "overlapping" || mode == "independent_keys" || mode == "warm_fill" {
						for b.Loop() {
							if mode == "warm_fill" {
								b.StopTimer()
								fs.FlushCache(ctx)
								b.StartTimer()
							}
							var reads [8]IOVector
							done := make(chan error, len(reads))
							start := make(chan struct{})
							var ready sync.WaitGroup
							for i := range reads {
								reads[i] = makeRead()
								if mode == "independent_keys" {
									reads[i].FilePath = paths[i]
								}
								ready.Add(1)
								go func(i int) {
									ready.Done()
									<-start
									done <- fs.Read(ctx, &reads[i])
								}(i)
							}
							ready.Wait()
							close(start)
							for range reads {
								require.NoError(b, <-done)
							}
							for i := range reads {
								reads[i].Release()
							}
						}
					} else {
						for b.Loop() {
							read := makeRead()
							err := fs.Read(ctx, &read)
							if err != nil {
								b.Fatal(err)
							}
							read.Release()
						}
					}
					b.ReportMetric(float64(calls.Load())/float64(b.N), "decodes/op")
					b.ReportMetric(float64(counters.FileService.Cache.Disk.Read.Load()-beforeDisk)/float64(b.N), "disk_reads/op")
					b.ReportMetric(float64(counters.FileService.Cache.Memory.Hit.Load()-beforeMemory)/float64(b.N), "mem_hits/op")
					b.ReportMetric(float64(counters.FileService.S3.Get.Load()-beforeS3)/float64(b.N), "s3_gets/op")
				})
			}
		})
	}
}
