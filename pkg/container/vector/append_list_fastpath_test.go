// Copyright 2021 Matrix Origin
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

func TestAppendFixedListFastPath(t *testing.T) {
	values := make([]int64, 32)
	for i := range values {
		values[i] = int64(i * 11)
	}
	for _, mask := range [][]bool{nil, make([]bool, len(values))} {
		mp := mpool.MustNewZero()
		vec := NewVec(types.T_int64.ToType())
		t.Cleanup(func() {
			defer mpool.DeleteMPool(mp)
			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
			require.Zero(t, mp.OnHeapCurrNB())
		})
		require.NoError(t, vec.PreExtend(len(values), mp))
		require.NoError(t, AppendFixedList(vec, values, mask, mp))
		require.Equal(t, values, MustFixedColWithTypeCheck[int64](vec))
		for i := range values {
			require.False(t, vec.IsNull(uint64(i)))
			require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(i))
			require.Equal(t, types.StringSourceExpression, vec.GetStringSourceAt(i))
		}
	}
}

func TestAppendFixedListFastPathWidths(t *testing.T) {
	ints8 := make([]int8, 32)
	ints32 := make([]int32, 32)
	decimals := make([]types.Decimal128, 32)
	for i := range ints8 {
		ints8[i] = int8(i)
		ints32[i] = int32(i * 100)
		decimals[i] = types.Decimal128{B0_63: uint64(i), B64_127: uint64(i + 100)}
	}
	checkAppendFixedListWidth(t, types.T_int8.ToType(), ints8)
	checkAppendFixedListWidth(t, types.T_int32.ToType(), ints32)
	checkAppendFixedListWidth(t, types.T_decimal128.ToType(), decimals)
}

func TestAppendFixedListFastPathAfterNullablePrefix(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType())
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	require.NoError(t, AppendFixedList(vec, []int64{1, 2}, []bool{false, true}, mp))
	values := make([]int64, 32)
	for i := range values {
		values[i] = int64(i + 10)
	}
	require.NoError(t, AppendFixedList(vec, values, nil, mp))
	require.Equal(t, 34, vec.Length())
	require.Equal(t, int64(1), GetFixedAtNoTypeCheck[int64](vec, 0))
	require.True(t, vec.IsNull(1))
	for i, value := range values {
		require.False(t, vec.IsNull(uint64(i+2)))
		require.Equal(t, value, GetFixedAtNoTypeCheck[int64](vec, i+2))
		require.Equal(t, PrepareParamNone, vec.GetPrepareParamKindAt(i+2))
		require.Equal(t, types.StringSourceExpression, vec.GetStringSourceAt(i+2))
	}
}

func TestAppendFixedListShortMaskKeepsPartialPanic(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType())
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	values := make([]int64, 16)
	for i := range values {
		values[i] = int64(i + 1)
	}
	require.Panics(t, func() {
		_ = AppendFixedList(vec, values, make([]bool, len(values)-1), mp)
	})
	require.Equal(t, len(values), vec.Length())
	for i := 0; i < len(values)-1; i++ {
		require.Equal(t, values[i], GetFixedAtNoTypeCheck[int64](vec, i))
	}
	require.Zero(t, GetFixedAtNoTypeCheck[int64](vec, len(values)-1))
}

func checkAppendFixedListWidth[T any](t *testing.T, typ types.Type, values []T) {
	t.Helper()
	mp := mpool.MustNewZero()
	vec := NewVec(typ)
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	require.NoError(t, AppendFixedList(vec, values, nil, mp))
	require.Equal(t, values, MustFixedColWithTypeCheck[T](vec))
}

func TestAppendFixedListAliasedInput(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType())
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})

	require.NoError(t, vec.PreExtend(17, mp))
	require.NoError(t, AppendFixedList(vec, []int64{7}, nil, mp))
	require.GreaterOrEqual(t, vec.Capacity(), 17)
	backing := MustFixedColWithTypeCheck[int64](vec)[:17]
	for i := 1; i < len(backing); i++ {
		backing[i] = int64(i)
	}
	require.NoError(t, AppendFixedList(vec, backing[:16], nil, mp))
	// The former forward loop reads later source elements after writing them.
	for _, value := range MustFixedColWithTypeCheck[int64](vec) {
		require.Equal(t, int64(7), value)
	}
}

func TestAppendFixedListAliasedInputAfterGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_int64.ToType())
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})

	values := make([]int64, 16)
	for i := range values {
		values[i] = int64(i + 4)
	}
	require.NoError(t, AppendFixedList(vec, values, nil, mp))
	if remaining := vec.Capacity() - vec.Length(); remaining > 0 {
		filler := make([]int64, remaining)
		for i := range filler {
			filler[i] = int64(i + len(values) + 4)
		}
		require.NoError(t, AppendFixedList(vec, filler, nil, mp))
	}
	vals := MustFixedColWithTypeCheck[int64](vec)
	require.GreaterOrEqual(t, len(vals), 16)
	require.Equal(t, vec.Capacity(), len(vals))
	require.Greater(t, vec.Length()+len(vals), vec.Capacity())
	before := append([]int64(nil), vals...)
	require.NoError(t, AppendFixedList(vec, vals, nil, mp))
	require.Equal(t, append(before, before...), MustFixedColWithTypeCheck[int64](vec))
}
