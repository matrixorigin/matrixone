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

package vector

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTypedReadLogicalBoundsAndConstantRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	flat := NewVec(types.T_int64.ToType())
	defer flat.Free(mp)
	require.NoError(t, flat.PreExtend(32, mp))
	require.NoError(t, AppendFixedList(flat, []int64{11, 22}, nil, mp))
	require.Equal(t, int64(11), GetFixedAtNoTypeCheck[int64](flat, 0))
	require.Equal(t, int64(22), GetFixedAtNoTypeCheck[int64](flat, 1))
	for _, row := range []int{-1, 2, 31} {
		require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int64](flat, row) })
	}
	constant, err := NewConstFixed(types.T_int64.ToType(), int64(42), 2, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	for _, row := range []int{-1, 0, 1, 100} {
		require.Equal(t, int64(42), GetFixedAtNoTypeCheck[int64](constant, row))
	}
	flat.ResetWithSameType()
	require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int64](flat, 0) })
	constantNull := NewConstNull(types.T_int64.ToType(), 2, mp)
	defer constantNull.Free(mp)
	require.Panics(t, func() { _ = GetFixedAtNoTypeCheck[int64](constantNull, 0) })
}

func TestTypedReadBytesRetainsAliasesAndBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	vec := NewVec(types.T_varbinary.ToType())
	defer vec.Free(mp)
	require.NoError(t, vec.PreExtend(32, mp))
	values := [][]byte{[]byte("inline"), bytes.Repeat([]byte("x"), 64), {}}
	require.NoError(t, AppendBytesList(vec, values, nil, mp))
	for row, want := range values {
		require.Equal(t, want, vec.GetBytesAt(row))
	}
	for _, row := range []int{0, 1} {
		value := vec.GetBytesAt(row)
		value[0] = 'z'
		require.Equal(t, byte('z'), vec.GetBytesAt(row)[0])
	}
	for _, row := range []int{-1, 3, 31} {
		require.Panics(t, func() { _ = vec.GetBytesAt(row) })
	}
	constant, err := NewConstBytes(types.T_varbinary.ToType(), []byte("constant"), 2, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	for _, row := range []int{-1, 0, 1, 100} {
		require.Equal(t, []byte("constant"), constant.GetBytesAt(row))
	}
	vec.ResetWithSameType()
	require.Panics(t, func() { _ = vec.GetBytesAt(0) })
}

func TestTypedReadBorrowedFixedKeepsBacking(t *testing.T) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data, 11)
	binary.LittleEndian.PutUint64(data[8:], 22)
	releases := 0
	lease, err := NewRefCountedBufferLease(data, 16, func() { releases++ })
	require.NoError(t, err)
	defer lease.Release()
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), 2, data, lease)
	require.NoError(t, err)
	defer vec.Free(nil)
	require.Equal(t, int64(11), GetFixedAtNoTypeCheck[int64](vec, 0))
	require.Equal(t, int64(22), GetFixedAtNoTypeCheck[int64](vec, 1))
	require.True(t, vec.HasBorrowedBacking())
	require.Zero(t, releases)
}

var typedReadBenchSink int64

func BenchmarkVectorTypedRead(b *testing.B) {
	const rows = 2048
	for _, constant := range []bool{false, true} {
		name := "int64_flat_2048"
		if constant {
			name = "int64_const_2048"
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			vec := NewVec(types.T_int64.ToType())
			defer func() { vec.Free(mp) }()
			if constant {
				vec.Free(mp)
				var err error
				vec, err = NewConstFixed(types.T_int64.ToType(), int64(42), rows, mp)
				require.NoError(b, err)
			} else {
				values := make([]int64, rows)
				for i := range values {
					values[i] = 42
				}
				require.NoError(b, AppendFixedList(vec, values, nil, mp))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var sum int64
				for row := 0; row < rows; row++ {
					sum += GetFixedAtNoTypeCheck[int64](vec, row)
				}
				typedReadBenchSink = sum
			}
			b.StopTimer()
			require.Equal(b, int64(rows*42), typedReadBenchSink)
		})
	}
	b.Run("bool_flat_2048", func(b *testing.B) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		vec := NewVec(types.T_bool.ToType())
		defer vec.Free(mp)
		values := make([]bool, rows)
		for row := range values {
			values[row] = row%2 == 0
		}
		require.NoError(b, AppendFixedList(vec, values, nil, mp))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var sum int64
			for row := 0; row < rows; row++ {
				if GetFixedAtNoTypeCheck[bool](vec, row) {
					sum++
				}
			}
			typedReadBenchSink = sum
		}
		b.StopTimer()
		require.Equal(b, int64(rows/2), typedReadBenchSink)
	})
	for _, size := range []int{6, 64} {
		name := "bytes_inline_2048"
		if size == 64 {
			name = "bytes_area_2048"
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			vec := NewVec(types.T_varbinary.ToType())
			defer vec.Free(mp)
			values := make([][]byte, rows)
			for row := range values {
				values[row] = bytes.Repeat([]byte("x"), size)
			}
			require.NoError(b, AppendBytesList(vec, values, nil, mp))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var sum int64
				for row := 0; row < rows; row++ {
					value := vec.GetBytesAt(row)
					sum += int64(len(value)) + int64(value[0])
				}
				typedReadBenchSink = sum
			}
			b.StopTimer()
			require.Equal(b, int64(rows*(size+int('x'))), typedReadBenchSink)
		})
	}
}
