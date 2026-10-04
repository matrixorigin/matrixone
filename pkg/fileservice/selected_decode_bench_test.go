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

package fileservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

// The fixture models a combined filter/vector/projection request. Overlap is
// in consumer lifetimes, not scheduling: all eight IOVectors remain live until
// the last read completes. Storage ranges and decoded native bytes are counted
// separately from Go allocations; this is not a SQL workload benchmark.
func BenchmarkSelectedDecode(b *testing.B) {
	for _, mode := range []string{"overlap", "single", "unmarked", "hot_cache"} {
		b.Run(mode, func(b *testing.B) {
			ctx := context.Background()
			capacity := toml.ByteSize(4 << 20)
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "fused-bench", Endpoint: "disk", Bucket: b.TempDir()},
				CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
			require.NoError(b, err)
			b.Cleanup(func() { fs.Close(ctx) })
			raw := make([]byte, 1<<20)
			for i := 0; i < len(raw)/4; i++ {
				binary.LittleEndian.PutUint32(raw[i*4:], uint32(i%768)*7919)
			}
			compressed := make([]byte, lz4.CompressBlockBound(len(raw)))
			n, err := lz4.CompressBlock(raw, compressed, nil)
			require.NoError(b, err)
			require.Positive(b, n)
			payload := append(bytes.Repeat([]byte{1}, 32), compressed[:n]...)
			payload = append(payload, bytes.Repeat([]byte{2}, 128)...)
			storage := &selectedDecodeBenchStorage{data: payload}
			fs.storage = storage
			if mode != "hot_cache" {
				pinned := fs.AllocateCacheData(ctx, int(capacity))
				b.Cleanup(pinned.Release)
			}
			var conversions, decodedBytes int64
			convert := func(ctx context.Context, _ io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
				conversions++
				out := a.AllocateCacheData(ctx, len(raw))
				decodedBytes += out.Capacity()
				if _, err := lz4.UncompressBlock(data, out.Bytes()); err != nil {
					out.Release()
					return nil, err
				}
				return out, nil
			}
			readers := 8
			if mode == "single" {
				readers = 1
			}
			vectors := make([]IOVector, readers)
			run := func() {
				for i := range vectors {
					vectors[i] = IOVector{FilePath: "columns", Policy: SkipFullFilePreloads, Entries: []IOEntry{
						{Size: 32, CachedDataSize: 32, ToCacheData: CacheOriginalData},
						{Offset: 32, Size: int64(n), CachedDataSize: int64(len(raw)), ToCacheData: convert},
						{Offset: int64(32 + n), Size: 128, CachedDataSize: 128, ToCacheData: CacheOriginalData},
					}}
					if mode != "unmarked" {
						vectors[i].Entries[1].DecodeSharing = DecodeSharing{Codec: "bench-lz4-v1"}
					}
					if err := fs.Read(ctx, &vectors[i]); err != nil {
						b.Fatal(err)
					}
				}
				for i := range vectors {
					vectors[i].Release()
					vectors[i] = IOVector{}
				}
			}
			b.Cleanup(func() {
				for i := range vectors {
					vectors[i].ReleaseReadResultOnError()
				}
			})
			run()
			storage.gets, storage.readBytes, conversions, decodedBytes = 0, 0, 0, 0
			var before, after syscall.Rusage
			require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &before))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
			require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &after))
			b.ReportMetric(float64(after.Utime.Nano()+after.Stime.Nano()-before.Utime.Nano()-before.Stime.Nano())/float64(b.N), "cpu-ns/op")
			b.ReportMetric(float64(conversions)/float64(b.N), "decodes/op")
			b.ReportMetric(float64(decodedBytes)/float64(b.N), "decoded-B/op")
			b.ReportMetric(float64(storage.gets)/float64(b.N), "gets/op")
			b.ReportMetric(float64(storage.readBytes)/float64(b.N), "read-B/op")
		})
	}
}

type selectedDecodeBenchStorage struct {
	dummyObjectStorage
	data            []byte
	gets, readBytes int64
}

func (s *selectedDecodeBenchStorage) Read(_ context.Context, _ string, min, max *int64) (io.ReadCloser, error) {
	start, end := int64(0), int64(len(s.data))
	if min != nil {
		start = *min
	}
	if max != nil {
		end = *max
	}
	s.gets++
	s.readBytes += end - start
	return io.NopCloser(bytes.NewReader(s.data[start:end])), nil
}
