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

package readutil

import (
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func checkFixedUnsortedIN[T any](t *testing.T, oid types.T, values, rows []T, equal func(T, T) bool, sorted bool) {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	needles := vector.NewVec(oid.ToType())
	t.Cleanup(func() { needles.Free(mp) })
	input := vector.NewVec(oid.ToType())
	t.Cleanup(func() { input.Free(mp) })
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(needles, value, false, mp))
	}
	needles.SetSorted(sorted)
	for _, value := range rows {
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}
	filter, err := ConstructBlockPKFilter(true, BasePKFilter{Valid: true, Op: function.IN, Oid: oid, Vec: needles}, nil)
	require.NoError(t, err)
	if filter.Cleanup != nil {
		t.Cleanup(filter.Cleanup)
	}
	require.True(t, filter.Valid)
	require.Nil(t, filter.CachedSearch)
	var want []int64
	for row, value := range rows {
		for _, needle := range values {
			if equal(value, needle) {
				want = append(want, int64(row))
				break
			}
		}
	}
	// The fake-PK decision must use unsorted rows even for a sorted block.
	search := filter.DecideSearchFunc(true)
	require.NotNil(t, search)
	require.Equal(t, want, search(containers.Vectors{*input}))
}

func TestUnsortedPKInSearch(t *testing.T) {
	t.Run("uint64 boundaries and duplicates", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_uint64,
			[]uint64{0, 1, 3, 3, 12, math.MaxUint64},
			[]uint64{12, 2, math.MaxUint64, 0, 3, 3}, func(a, b uint64) bool { return a == b }, true)
	})
	t.Run("signed boundaries", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_int64,
			[]int64{math.MinInt64, -4, -1, 0, 9, math.MaxInt64},
			[]int64{9, -2, math.MinInt64, math.MaxInt64, -1}, func(a, b int64) bool { return a == b }, true)
	})
	t.Run("float zero and nan row", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_float64,
			[]float64{-5, -2, math.Copysign(0, -1), 1, 8},
			[]float64{0, math.NaN(), 8, 2, math.Copysign(0, -1)}, func(a, b float64) bool { return a == b }, true)
	})
	t.Run("decimal comparator", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_decimal128,
			[]types.Decimal128{{B0_63: 0}, {B0_63: 1}, {B0_63: 2}, {B0_63: 3}, {B0_63: 4}, {B0_63: 5}},
			[]types.Decimal128{{B0_63: 5}, {B0_63: 7}, {B0_63: 1}},
			func(a, b types.Decimal128) bool { return types.CompareDecimal128(a, b) == 0 }, true)
	})
	t.Run("uuid comparator", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_uuid,
			[]types.Uuid{{0}, {1}, {2}, {3}, {4}, {5}},
			[]types.Uuid{{5}, {7}, {1}, {0}},
			func(a, b types.Uuid) bool { return types.CompareUuid(a, b) == 0 }, true)
	})
	t.Run("bool duplicate needles", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_bool,
			[]bool{false, false, false, true, true}, []bool{true, false, true},
			func(a, b bool) bool { return a == b }, true)
	})
	t.Run("unnormalized needles retain exact search", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_uint64,
			[]uint64{10, 1, 8, 3, 7, 2}, []uint64{2, 10, 5, 3},
			func(a, b uint64) bool { return a == b }, false)
	})
	t.Run("empty needles", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_uint64, nil, []uint64{0, 1}, func(a, b uint64) bool { return a == b }, true)
	})
}

func TestUnsortedPKInBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	needles := vector.NewVec(types.T_varbinary.ToType())
	t.Cleanup(func() { needles.Free(mp) })
	rows := vector.NewVec(types.T_varbinary.ToType())
	t.Cleanup(func() { rows.Free(mp) })
	for _, value := range [][]byte{{0}, {0, 1}, {1}, {1}, {2}, {255}} {
		require.NoError(t, vector.AppendBytes(needles, value, false, mp))
	}
	needles.SetSorted(true)
	for _, value := range [][]byte{{2}, {3}, {0, 1}, {255}, {1}} {
		require.NoError(t, vector.AppendBytes(rows, value, false, mp))
	}
	filter, err := ConstructBlockPKFilter(true, BasePKFilter{Valid: true, Op: function.IN, Oid: types.T_varbinary, Vec: needles}, nil)
	require.NoError(t, err)
	require.True(t, filter.Valid)
	require.Equal(t, []int64{0, 2, 3, 4}, filter.DecideSearchFunc(true)(containers.Vectors{*rows}))
}

func TestUnsortedPKInArrayUsesByteEquality(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float64} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			needles := vector.NewVec(oid.ToType())
			t.Cleanup(func() { needles.Free(mp) })
			rows := vector.NewVec(oid.ToType())
			t.Cleanup(func() { rows.Free(mp) })
			for i := 1; i <= 6; i++ {
				if oid == types.T_array_float32 {
					require.NoError(t, vector.AppendArray(needles, []float32{float32(i)}, false, mp))
				} else {
					require.NoError(t, vector.AppendArray(needles, []float64{float64(i)}, false, mp))
				}
			}
			require.NoError(t, needles.InplaceSortAndCompact(mp))
			require.True(t, needles.GetSorted())
			if oid == types.T_array_float32 {
				require.NoError(t, vector.AppendArray(rows, []float32{2}, false, mp))
				require.NoError(t, vector.AppendArray(rows, []float32{7}, false, mp))
			} else {
				require.NoError(t, vector.AppendArray(rows, []float64{2}, false, mp))
				require.NoError(t, vector.AppendArray(rows, []float64{7}, false, mp))
			}
			filter, err := ConstructBlockPKFilter(true, BasePKFilter{Valid: true, Op: function.IN, Oid: oid, Vec: needles}, nil)
			require.NoError(t, err)
			require.True(t, filter.Valid)
			require.Equal(t, []int64{0}, filter.DecideSearchFunc(true)(containers.Vectors{*rows}))
		})
	}
}

func TestUnsortedPKInSearchComparisonBound(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	rows := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { rows.Free(mp) })
	for _, value := range []int64{1023, 2048, -1, 511} {
		require.NoError(t, vector.AppendFixed(rows, value, false, mp))
	}
	values := make([]int64, 1024)
	for i := range values {
		values[i] = int64(i)
	}
	comparisons := 0
	search := fixedInUnsortedSearch(values, func(a, b int64) int {
		comparisons++
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})
	require.Equal(t, []int64{0, 3}, search(rows))
	require.LessOrEqual(t, comparisons, 4*12)
}

func BenchmarkUnsortedPKInSearch(b *testing.B) {
	for _, count := range []int{1000, 2000, 5000} {
		b.Run(fmt.Sprintf("keys_%d", count), func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { mpool.DeleteMPool(mp) })
			needles := vector.NewVec(types.T_uint64.ToType())
			b.Cleanup(func() { needles.Free(mp) })
			rows := vector.NewVec(types.T_uint64.ToType())
			b.Cleanup(func() { rows.Free(mp) })
			for i := 0; i < count; i++ {
				require.NoError(b, vector.AppendFixed(needles, uint64(i), false, mp))
			}
			needles.SetSorted(true)
			for i := 0; i < 8192; i++ {
				require.NoError(b, vector.AppendFixed(rows, uint64(i+count), false, mp))
			}
			filter, err := ConstructBlockPKFilter(true, BasePKFilter{Valid: true, Op: function.IN, Oid: types.T_uint64, Vec: needles}, nil)
			require.NoError(b, err)
			search := filter.DecideSearchFunc(true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if len(search(containers.Vectors{*rows})) != 0 {
					b.Fatal("unexpected match")
				}
			}
		})
	}
}
