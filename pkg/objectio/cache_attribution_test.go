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
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColumnConstructionAttribution(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	source := vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { source.Free(mp) })
	for i := 0; i < 64; i++ {
		require.NoError(t, vector.AppendFixed(source, int32(0), false, mp))
	}
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	encoded := append(append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_V2})...), payload...)
	compressed, err := compress.Compress(encoded, make([]byte, lz4.CompressBlockBound(len(encoded))), compress.Lz4)
	require.NoError(t, err)
	require.NotEmpty(t, compressed)
	for _, tc := range []struct {
		name string
		algo uint8
		data []byte
		fail bool
	}{
		{"uncompressed", compress.None, encoded, false},
		{"lz4", compress.Lz4, compressed, false},
		{"validation_error", compress.None, []byte{0}, true},
		{"decompression_error", compress.Lz4, []byte("invalid-lz4"), true},
		{"chunk_header_error", compress.Lz4Chunked, []byte{0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var counters perfcounter.CounterSet
			ctx := perfcounter.WithCounterSet(context.Background(), &counters)
			data, err := columnCacheConstructorFactory(int64(len(encoded)), tc.algo)(ctx, bytes.NewReader(tc.data), tc.data, fileservice.DefaultCacheDataAllocator())
			if data != nil {
				defer data.Release()
			}
			counts := &counters.FileService.ColumnConstruction.Uncompressed
			if tc.algo == compress.Lz4 {
				counts = &counters.FileService.ColumnConstruction.LZ4
			}
			if tc.algo == compress.Lz4Chunked {
				counts = &counters.FileService.ColumnConstruction.Chunked
			}
			require.EqualValues(t, 1, counts.Calls.Load())
			if tc.fail {
				require.Error(t, err)
				require.Nil(t, data)
				require.EqualValues(t, 1, counts.Errors.Load())
				require.Zero(t, counts.Bytes.Load())
			} else {
				require.NoError(t, err)
				require.EqualValues(t, len(encoded), counts.Bytes.Load())
				require.Zero(t, counts.Errors.Load())
			}
		})
	}
}

func TestColumnCacheHitDoesNotReconstruct(t *testing.T) {
	var counters perfcounter.CounterSet
	ctx := perfcounter.WithCounterSet(context.Background(), &counters)
	capacity := toml.ByteSize(1 << 20)
	fs, err := fileservice.NewLocalFS(ctx, "attribution", t.TempDir(), fileservice.CacheConfig{MemoryCapacity: &capacity}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	source := vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, vector.AppendFixed(source, int32(42), false, mp))
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	encoded := append(append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_V2})...), payload...)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{FilePath: "column", Entries: []fileservice.IOEntry{{Size: int64(len(encoded)), Data: encoded}}, Policy: fileservice.SkipAllCache}))
	counters.Reset()
	for i := 0; i < 2; i++ {
		func() {
			ioVec := fileservice.IOVector{FilePath: "column", Policy: fileservice.SkipFullFilePreloads, Entries: []fileservice.IOEntry{{Size: int64(len(encoded)), ToCacheData: columnCacheConstructorFactory(int64(len(encoded)), compress.None)}}}
			defer ioVec.Release()
			require.NoError(t, fs.Read(ctx, &ioVec))
			require.EqualValues(t, 1, counters.FileService.ColumnConstruction.Uncompressed.Calls.Load())
			require.EqualValues(t, 1, counters.FileService.Cache.Memory.UpdateAdmitted.Load())
		}()
	}
	require.EqualValues(t, 1, counters.FileService.Cache.Memory.Hit.Load())
}

func BenchmarkColumnCacheConstructionAttribution(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := vector.NewVec(types.T_int32.ToType())
	defer source.Free(mp)
	for i := 0; i < 4096; i++ {
		if err := vector.AppendFixed(source, int32(i%8), false, mp); err != nil {
			b.Fatal(err)
		}
	}
	payload, err := source.MarshalBinary()
	require.NoError(b, err)
	encoded := append(append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_V2})...), payload...)
	compressed, err := compress.Compress(encoded, make([]byte, lz4.CompressBlockBound(len(encoded))), compress.Lz4)
	require.NoError(b, err)
	require.NotEmpty(b, compressed)
	for _, tc := range []struct {
		name string
		algo uint8
		data []byte
	}{{"uncompressed", compress.None, encoded}, {"lz4", compress.Lz4, compressed}} {
		b.Run(tc.name, func(b *testing.B) {
			construct := columnCacheConstructorFactory(int64(len(encoded)), tc.algo)
			allocator := fileservice.DefaultCacheDataAllocator()
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := construct(ctx, nil, tc.data, allocator)
				if err != nil {
					b.Fatal(err)
				}
				data.Release()
			}
		})
	}
}
