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

package aggexec

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func makeOffHeapFixedVector[T any](
	t testing.TB,
	mp *mpool.MPool,
	typ types.Type,
	values []T,
	nulls []bool,
) *vector.Vector {
	t.Helper()
	vec := vector.NewOffHeapVecWithType(typ)
	require.NoError(t, vector.AppendFixedList(vec, values, nulls, mp))
	return vec
}

func makeOffHeapVarcharVector(
	t testing.TB,
	mp *mpool.MPool,
	values []string,
	nulls []bool,
) *vector.Vector {
	t.Helper()
	vec := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vector.AppendStringList(vec, values, nulls, mp))
	return vec
}

func TestMergeSplitResult(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		result, err := MergeSplitResult(nil, mpool.MustNewZero())
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("single nil chunk", func(t *testing.T) {
		result, err := MergeSplitResult([]*vector.Vector{nil}, mpool.MustNewZero())
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("single chunk is zero copy", func(t *testing.T) {
		mp := mpool.MustNewZero()
		input := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{1, 2, 3}, nil)
		result, err := MergeSplitResult([]*vector.Vector{input}, mp)
		require.NoError(t, err)
		require.Same(t, input, result)
		result.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("fixed chunks preserve order and nulls", func(t *testing.T) {
		mp := mpool.MustNewZero()
		first := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{1, 2}, []bool{false, true})
		second := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{3, 4}, []bool{true, false})
		third := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{5, 6}, nil)
		result, err := MergeSplitResult([]*vector.Vector{first, second, third}, mp)
		require.NoError(t, err)
		require.Same(t, first, result)
		require.Equal(t, 6, result.Length())
		// A NULL row's payload is deliberately unspecified; its bitmap is the
		// semantic value. Non-NULL payloads must remain ordered.
		values := vector.MustFixedColWithTypeCheck[int32](result)
		require.Equal(t, int32(1), values[0])
		require.Equal(t, int32(4), values[3])
		require.False(t, result.GetNulls().Contains(0))
		require.True(t, result.GetNulls().Contains(1))
		require.True(t, result.GetNulls().Contains(2))
		require.False(t, result.GetNulls().Contains(3))
		require.Equal(t, int32(5), values[4])
		require.Equal(t, int32(6), values[5])
		result.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("varlen chunks preserve inline and area values", func(t *testing.T) {
		mp := mpool.MustNewZero()
		longA := "this value is longer than the inline varlena limit A"
		longB := "this value is longer than the inline varlena limit B"
		first := makeOffHeapVarcharVector(t, mp, []string{"a", longA}, nil)
		second := makeOffHeapVarcharVector(t, mp, []string{"", longB, "z"}, []bool{true, false, false})
		result, err := MergeSplitResult([]*vector.Vector{first, second}, mp)
		require.NoError(t, err)
		require.Equal(t, 5, result.Length())
		require.Equal(t, "a", string(result.GetBytesAt(0)))
		require.Equal(t, longA, string(result.GetBytesAt(1)))
		require.True(t, result.GetNulls().Contains(2))
		require.Equal(t, longB, string(result.GetBytesAt(3)))
		require.Equal(t, "z", string(result.GetBytesAt(4)))
		result.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("nil chunk frees all inputs", func(t *testing.T) {
		mp := mpool.MustNewZero()
		first := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{1}, nil)
		last := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{2}, nil)
		result, err := MergeSplitResult([]*vector.Vector{first, nil, last}, mp)
		require.Error(t, err)
		require.Nil(t, result)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("nil first chunk frees remaining inputs", func(t *testing.T) {
		mp := mpool.MustNewZero()
		last := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{2}, nil)
		result, err := MergeSplitResult([]*vector.Vector{nil, last}, mp)
		require.Error(t, err)
		require.Nil(t, result)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("mismatched types free all inputs", func(t *testing.T) {
		mp := mpool.MustNewZero()
		first := makeOffHeapFixedVector(t, mp, types.T_int32.ToType(), []int32{1}, nil)
		second := vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(second, int64(2), false, mp))
		result, err := MergeSplitResult([]*vector.Vector{first, second}, mp)
		require.Error(t, err)
		require.Nil(t, result)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("allocation failure frees all inputs", func(t *testing.T) {
		const limit = 1 << 20
		mp, err := mpool.NewMPool("merge-split-limited", limit, mpool.NoFixed)
		require.NoError(t, err)
		var filler []byte
		defer func() {
			if filler != nil {
				mp.Free(filler)
			}
			mpool.DeleteMPool(mp)
		}()
		first := makeOffHeapFixedVector(t, mp, types.T_int64.ToType(), []int64{1}, nil)
		second := makeOffHeapFixedVector(t, mp, types.T_int64.ToType(), make([]int64, 1024), nil)
		remaining := int64(limit) - mp.CurrNB()
		require.Greater(t, remaining, int64(1024))
		filler, err = mp.Alloc(int(remaining-64), true)
		require.NoError(t, err)
		usedByFiller := mp.CurrNB() - int64(first.Allocated()) - int64(second.Allocated())

		result, err := MergeSplitResult([]*vector.Vector{first, second}, mp)
		if result != nil {
			result.Free(mp)
		}
		require.Error(t, err)
		require.Nil(t, result)
		require.Equal(t, usedByFiller, mp.CurrNB(), "failed merge must release both source vectors")

		mp.Free(filler)
		filler = nil
		require.Equal(t, int64(0), mp.CurrNB())
	})
}

func BenchmarkMergeSplitResult(b *testing.B) {
	values := make([]int64, AggBatchSize)
	for i := range values {
		values[i] = int64(i)
	}

	b.Run("fixed/1_chunk", func(b *testing.B) {
		mp := mpool.MustNewZero()
		result := makeOffHeapFixedVector(b, mp, types.T_int64.ToType(), values, nil)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var err error
			result, err = MergeSplitResult([]*vector.Vector{result}, mp)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		result.Free(mp)
		require.Equal(b, int64(0), mp.CurrNB())
	})

	for _, chunks := range []int{2, 4} {
		b.Run(fmt.Sprintf("fixed/%d_chunks", chunks), func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				vecs := make([]*vector.Vector, chunks)
				for j := range vecs {
					vecs[j] = makeOffHeapFixedVector(b, mp, types.T_int64.ToType(), values, nil)
				}
				b.StartTimer()
				result, err := MergeSplitResult(vecs, mp)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				result.Free(mp)
			}
			require.Equal(b, int64(0), mp.CurrNB())
		})
	}

	b.Run("varlen/2_chunks", func(b *testing.B) {
		const chunkRows = 1024
		mp := mpool.MustNewZero()
		values := make([]string, chunkRows)
		for i := range values {
			values[i] = fmt.Sprintf("row-%04d-with-non-inline-payload", i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			vecs := []*vector.Vector{
				makeOffHeapVarcharVector(b, mp, values, nil),
				makeOffHeapVarcharVector(b, mp, values, nil),
			}
			b.StartTimer()
			result, err := MergeSplitResult(vecs, mp)
			if err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			result.Free(mp)
		}
		require.Equal(b, int64(0), mp.CurrNB())
	})
}
