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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func BenchmarkColumnCacheConstruction(b *testing.B) {
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
