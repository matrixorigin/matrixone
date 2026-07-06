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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// exerciseNarrowArray drives the generic narrow-vector (bf16/f16/int8/uint8) array
// functions added for quantized vector columns: Append*, GetArrayAt, NewConstArray,
// SetConstArray, and the String()/RowToString() narrow branches.
func exerciseNarrowArray[T types.ArrayElement](t *testing.T, oid types.T, a, b []T) {
	mp := mpool.MustNewZero()
	dim := int32(len(a))

	// AppendArray (value + null) then AppendArrayList
	vec := NewVec(types.New(oid, dim, 0))
	require.NoError(t, AppendArray[T](vec, a, false, mp))
	require.NoError(t, AppendArray[T](vec, nil, true, mp)) // null row
	require.NoError(t, AppendArrayList[T](vec, [][]T{a, b}, nil, mp))
	require.Equal(t, a, GetArrayAt[T](vec, 0))
	require.Equal(t, b, GetArrayAt[T](vec, 3))
	_ = vec.String()          // String() narrow branch (multi-row + null bitmap)
	_ = vec.RowToString(0)    // RowToString -> implArrayRowToString narrow branch
	_ = vec.RowToString(2)
	vec.Free(mp)

	// NewConstArray + GetArrayAt + single-row String()
	cv, err := NewConstArray[T](types.New(oid, dim, 0), a, 2, mp)
	require.NoError(t, err)
	require.Equal(t, a, GetArrayAt[T](cv, 0))
	_ = cv.String()
	_ = cv.RowToString(0)
	cv.Free(mp)

	// SetConstArray
	sv := NewVec(types.New(oid, dim, 0))
	require.NoError(t, SetConstArray[T](sv, b, 3, mp))
	require.Equal(t, b, GetArrayAt[T](sv, 0))
	sv.Free(mp)

	// single-row String()/RowToString — the len(col)==1 narrow branch
	one := NewVec(types.New(oid, dim, 0))
	require.NoError(t, AppendArray[T](one, a, false, mp))
	_ = one.String()
	_ = one.RowToString(0)
	one.Free(mp)

	// InplaceSort narrow branch (unsorted input with a duplicate)
	srt := NewVec(types.New(oid, dim, 0))
	require.NoError(t, AppendArrayList[T](srt, [][]T{b, a, a}, nil, mp))
	srt.InplaceSort()
	srt.Free(mp)

	// InplaceSortAndCompact narrow branch (sort + dedup)
	cmp := NewVec(types.New(oid, dim, 0))
	require.NoError(t, AppendArrayList[T](cmp, [][]T{b, a, a}, nil, mp))
	cmp.InplaceSortAndCompact()
	cmp.Free(mp)

	// GetMinMaxValue narrow branch
	mm := NewVec(types.New(oid, dim, 0))
	require.NoError(t, AppendArrayList[T](mm, [][]T{a, b}, nil, mp))
	_, _, _ = mm.GetMinMaxValue()
	mm.Free(mp)
}

func TestNarrowArrayVectorOps(t *testing.T) {
	t.Run("bf16", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_bf16,
			types.Float32ToBF16Slice([]float32{1, 2, 3}), types.Float32ToBF16Slice([]float32{4, 5, 6}))
	})
	t.Run("f16", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_float16,
			types.Float32ToFloat16Slice([]float32{1, 2, 3}), types.Float32ToFloat16Slice([]float32{4, 5, 6}))
	})
	t.Run("int8", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_int8, []int8{1, 2, 3}, []int8{4, 5, 6})
	})
	t.Run("uint8", func(t *testing.T) {
		exerciseNarrowArray(t, types.T_array_uint8, []uint8{1, 2, 3}, []uint8{4, 5, 6})
	})
}
