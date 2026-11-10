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
	"slices"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	t.Run("float32 signed zero infinities and nan row", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_float32,
			[]float32{float32(math.Inf(-1)), -2, float32(math.Copysign(0, -1)), 0, 2, float32(math.Inf(1))},
			[]float32{0, float32(math.Copysign(0, -1)), float32(math.NaN()), float32(math.Inf(1)), -2, 3, float32(math.Inf(-1))},
			func(a, b float32) bool { return a == b }, true)
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

func TestUnsortedPKInOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	needles := vector.NewVec(types.T_uint64.ToType())
	t.Cleanup(func() { needles.Free(mp) })
	for i := 0; i < 1000; i++ {
		require.NoError(t, vector.AppendFixed(needles, uint64(4*(i/2)), false, mp))
	}
	require.NoError(t, vector.AppendFixed(needles, uint64(math.MaxUint64), false, mp))
	needles.SetSorted(true)
	filter, err := ConstructBlockPKFilter(true, BasePKFilter{Valid: true, Op: function.IN, Oid: types.T_uint64, Vec: needles}, nil)
	require.NoError(t, err)
	if filter.Cleanup != nil {
		t.Cleanup(filter.Cleanup)
	}
	require.True(t, filter.Valid)
	search := filter.DecideSearchFunc(true) // fake PK must ignore block sort metadata
	base := make([]uint64, objectio.BlockMaxRows)
	for i := range base {
		base[i] = uint64(i / 4)
	}
	base[len(base)-1] = math.MaxUint64
	for _, tc := range []struct {
		name   string
		mutate func([]uint64)
	}{
		{"ordered", func([]uint64) {}},
		{"first inversion", func(rows []uint64) { rows[0] = math.MaxUint64 }},
		{"middle inversion", func(rows []uint64) { rows[len(rows)/2] = math.MaxUint64 }},
		{"last inversion", func(rows []uint64) { rows[len(rows)-1] = 0 }},
		{"ordered after fallback", func([]uint64) {}},
		{"all equal", func(rows []uint64) {
			for i := range rows {
				rows[i] = 20
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := slices.Clone(base)
			tc.mutate(data)
			rows := vector.NewVec(types.T_uint64.ToType())
			t.Cleanup(func() { rows.Free(mp) })
			var want []int64
			for i, value := range data {
				require.NoError(t, vector.AppendFixed(rows, value, false, mp))
				// Independent arithmetic oracle for the generated candidate set.
				if value == math.MaxUint64 || value <= 1996 && value%4 == 0 {
					want = append(want, int64(i))
				}
			}
			require.Equal(t, want, search(containers.Vectors{*rows}))
		})
	}
}

func TestUnsortedPKInOrderSigned(t *testing.T) {
	values := []int32{math.MinInt32}
	for i := -32; i < 32; i++ {
		values = append(values, int32(i))
	}
	values = append(values, math.MaxInt32)
	rows := make([]int32, objectio.BlockMaxRows)
	for i := range rows {
		rows[i] = int32(i/8 - 512)
	}
	rows[0], rows[len(rows)-1] = math.MinInt32, math.MaxInt32
	checkFixedUnsortedIN(t, types.T_int32, values, rows, func(a, b int32) bool { return a == b }, true)
}

func checkUnsortedPKInIntegerType[T orderedIntegerBacked](t *testing.T, oid types.T) {
	t.Helper()
	values := make([]T, minUnsortedINHashKeys)
	for i := range values {
		values[i] = T(i)
	}
	rows := make([]T, objectio.BlockMaxRows/2)
	for i := range rows {
		rows[i] = T(i / 64)
	}
	checkFixedUnsortedIN(t, oid, values, rows, func(a, b T) bool { return a == b }, true)
	rows[len(rows)-1] = 0
	checkFixedUnsortedIN(t, oid, values, rows, func(a, b T) bool { return a == b }, true)
}

func TestUnsortedPKInIntegerTypes(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{"bit", func(t *testing.T) { checkUnsortedPKInIntegerType[uint64](t, types.T_bit) }},
		{"int8", func(t *testing.T) { checkUnsortedPKInIntegerType[int8](t, types.T_int8) }},
		{"int16", func(t *testing.T) { checkUnsortedPKInIntegerType[int16](t, types.T_int16) }},
		{"int32", func(t *testing.T) { checkUnsortedPKInIntegerType[int32](t, types.T_int32) }},
		{"int64", func(t *testing.T) { checkUnsortedPKInIntegerType[int64](t, types.T_int64) }},
		{"uint8", func(t *testing.T) { checkUnsortedPKInIntegerType[uint8](t, types.T_uint8) }},
		{"uint16", func(t *testing.T) { checkUnsortedPKInIntegerType[uint16](t, types.T_uint16) }},
		{"uint32", func(t *testing.T) { checkUnsortedPKInIntegerType[uint32](t, types.T_uint32) }},
		{"uint64", func(t *testing.T) { checkUnsortedPKInIntegerType[uint64](t, types.T_uint64) }},
		{"date", func(t *testing.T) { checkUnsortedPKInIntegerType[types.Date](t, types.T_date) }},
		{"time", func(t *testing.T) { checkUnsortedPKInIntegerType[types.Time](t, types.T_time) }},
		{"datetime", func(t *testing.T) { checkUnsortedPKInIntegerType[types.Datetime](t, types.T_datetime) }},
		{"timestamp", func(t *testing.T) { checkUnsortedPKInIntegerType[types.Timestamp](t, types.T_timestamp) }},
		{"year", func(t *testing.T) { checkUnsortedPKInIntegerType[types.MoYear](t, types.T_year) }},
		{"enum", func(t *testing.T) { checkUnsortedPKInIntegerType[types.Enum](t, types.T_enum) }},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

func TestUnsortedPKInOrderInRangeBoundary(t *testing.T) {
	// The sorted vector search switches from binary probes to a merge when
	// more than 64 candidates fall within the physical row range.
	for _, count := range []int{64, 65} {
		t.Run(fmt.Sprintf("keys_%d", count), func(t *testing.T) {
			values := make([]uint64, count)
			for i := range values {
				values[i] = uint64(2 * i)
			}
			rows := make([]uint64, objectio.BlockMaxRows/2)
			for i := range rows {
				rows[i] = uint64(i)
			}
			checkFixedUnsortedIN(t, types.T_uint64, values, rows, func(a, b uint64) bool { return a == b }, true)
		})
	}
	t.Run("candidates below rows", func(t *testing.T) {
		values := make([]uint64, minUnsortedINHashKeys)
		for i := range values {
			values[i] = uint64(2 * i)
		}
		rows := make([]uint64, objectio.BlockMaxRows/2)
		for i := range rows {
			rows[i] = uint64(i + 256)
		}
		checkFixedUnsortedIN(t, types.T_uint64, values, rows, func(a, b uint64) bool { return a == b }, true)
	})
}

func TestUnsortedPKInOrderFallbacks(t *testing.T) {
	values := make([]uint64, minUnsortedINHashKeys)
	for i := range values {
		values[i] = uint64(2 * i)
	}
	t.Run("unnormalized candidates", func(t *testing.T) {
		unsorted := slices.Clone(values)
		slices.Reverse(unsorted)
		rows := make([]uint64, objectio.BlockMaxRows/2)
		for i := range rows {
			rows[i] = uint64(i)
		}
		checkFixedUnsortedIN(t, types.T_uint64, unsorted, rows, func(a, b uint64) bool { return a == b }, false)
	})
	t.Run("empty physical rows", func(t *testing.T) {
		checkFixedUnsortedIN(t, types.T_uint64, values, nil, func(a, b uint64) bool { return a == b }, true)
	})
}

func TestUnsortedPKInSearchBoundaries(t *testing.T) {
	for _, count := range []int{1, 4, 5, minUnsortedINHashKeys - 1, minUnsortedINHashKeys, maxUnsortedINHashKeys, maxUnsortedINHashKeys + 1} {
		t.Run(fmt.Sprintf("keys_%d", count), func(t *testing.T) {
			values := make([]uint64, count)
			for i := range values {
				values[i] = uint64(i * 2)
			}
			checkFixedUnsortedIN(t, types.T_uint64, values,
				[]uint64{uint64(count*2 - 2), 1, 0, math.MaxUint64, 0},
				func(a, b uint64) bool { return a == b }, true)
		})
	}
	for _, count := range []int{5, maxUnsortedINHashKeys + 1} {
		t.Run(fmt.Sprintf("float_rows_%d", count), func(t *testing.T) {
			values := make([]float64, count)
			for i := range values {
				values[i] = float64(i - 2)
			}

			checkFixedUnsortedIN(t, types.T_float64, values,
				[]float64{-2, -1, 0, math.Copysign(0, -1), math.NaN(), float64(count), float64(count - 4)},
				func(a, b float64) bool { return a == b }, true)
		})
	}
}

func TestUnsortedPKInSearchRowBoundaries(t *testing.T) {
	halfBlock := objectio.BlockMaxRows / 2
	for _, test := range []struct {
		name       string
		keys, rows int
	}{
		{"below key threshold", minUnsortedINHashKeys - 1, halfBlock},
		{"below row threshold", minUnsortedINHashKeys, halfBlock - 1},
		{"at row threshold", minUnsortedINHashKeys, halfBlock},
		{"above row threshold", minUnsortedINHashKeys, halfBlock + 1},
		{"below key-size threshold", 10000, 4999},
		{"at key-size threshold", 10000, 5000},
		{"partial block at key cap", maxUnsortedINHashKeys, objectio.BlockMaxRows - 1},
		{"full block at key cap", maxUnsortedINHashKeys, objectio.BlockMaxRows},
		{"above key cap", maxUnsortedINHashKeys + 1, objectio.BlockMaxRows},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			rows := vector.NewVec(types.T_uint64.ToType())
			t.Cleanup(func() { rows.Free(mp) })
			values := make([]uint64, test.keys)
			for i := range values {
				values[i] = uint64(2 * i)
			}
			var want []int64
			for i := 0; i < test.rows; i++ {
				value := uint64(2*(i%test.keys) + i%2)
				require.NoError(t, vector.AppendFixed(rows, value, false, mp))
				if i%2 == 0 {
					want = append(want, int64(i))
				}
			}
			_, search := orderedInSearchFuncs(values, true)
			require.Equal(t, want, search(rows))
		})
	}
}

func TestUnsortedPKInSearchConstantRows(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	rows, err := vector.NewConstFixed(types.T_uint64.ToType(), uint64(42), objectio.BlockMaxRows, mp)
	if rows != nil {
		t.Cleanup(func() { rows.Free(mp) })
	}
	require.NoError(t, err)
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i)
	}
	// A constant vector has one physical value, despite its logical length.
	require.Len(t, vector.MustFixedColNoTypeCheck[uint64](rows), 1)
	_, search := orderedInSearchFuncs(values, true)
	require.Equal(t, []int64{0}, search(rows))
}

func checkUnsortedPKInFloatFullBlock[T ~float32 | ~float64](t *testing.T, oid types.T) {
	t.Helper()
	values := []T{T(math.Inf(-1)), -1, T(math.Copysign(0, -1)), 0}
	for i := 1; i <= 64; i++ {
		values = append(values, T(i))
	}
	values = append(values, T(math.Inf(1)))
	inputs := []T{T(math.Inf(-1)), 0, T(math.Copysign(0, -1)), T(math.NaN()), 12, 65, T(math.Inf(1))}
	rows := make([]T, objectio.BlockMaxRows)
	for i := range rows {
		rows[i] = inputs[i%len(inputs)]
	}
	checkFixedUnsortedIN(t, oid, values, rows, func(a, b T) bool { return a == b }, true)
}

func TestUnsortedPKInFloatFullBlock(t *testing.T) {
	t.Run("float32", func(t *testing.T) { checkUnsortedPKInFloatFullBlock[float32](t, types.T_float32) })
	t.Run("float64", func(t *testing.T) { checkUnsortedPKInFloatFullBlock[float64](t, types.T_float64) })
}

func TestUnsortedPKInSearchConcurrentReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	small := vector.NewVec(types.T_uint64.ToType())
	t.Cleanup(func() { small.Free(mp) })
	for _, v := range []uint64{9, 8, 0, 5, 3, 5, math.MaxUint64} {
		require.NoError(t, vector.AppendFixed(small, v, false, mp))
	}
	large := vector.NewVec(types.T_uint64.ToType())
	t.Cleanup(func() { large.Free(mp) })
	var largeWant []int64
	for i := 0; i < objectio.BlockMaxRows; i++ {
		require.NoError(t, vector.AppendFixed(large, uint64(i%512), false, mp))
		if i%512 < 256 {
			largeWant = append(largeWant, int64(i))
		}
	}
	ordered := vector.NewVec(types.T_uint64.ToType())
	t.Cleanup(func() { ordered.Free(mp) })
	orderedWant := make([]int64, objectio.BlockMaxRows)
	for i := range orderedWant {
		require.NoError(t, vector.AppendFixed(ordered, uint64(i/32), false, mp))
		orderedWant[i] = int64(i)
	}
	values := make([]uint64, 256)
	for i := range values {
		values[i] = uint64(i)
	}
	orderedSearch, search := orderedInSearchFuncs(values, true)
	start := make(chan struct{})
	type result struct {
		offsets []int64
		kind    int
	}
	results := make(chan result, 16)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		kind := i % 4
		wg.Go(func() {
			<-start
			for j := 0; j < 2; j++ {
				switch kind {
				case 0:
					results <- result{offsets: search(large), kind: kind}
				case 1:
					results <- result{offsets: search(ordered), kind: kind}
				case 2:
					results <- result{offsets: orderedSearch(ordered), kind: kind}
				default:
					results <- result{offsets: search(small), kind: kind}
				}
			}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	for got := range results {
		switch got.kind {
		case 0:
			require.Equal(t, largeWant, got.offsets)
		case 1, 2:
			require.Equal(t, orderedWant, got.offsets)
		default:
			require.Equal(t, []int64{0, 1, 2, 3, 4, 5}, got.offsets)
		}
	}
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

// Measure an ordered block and the worst-case order proof before hash fallback.
func BenchmarkUnsortedPKInOrderedRows(b *testing.B) {
	for _, inverted := range []bool{false, true} {
		name := "ordered"
		if inverted {
			name = "last-inversion"
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { mpool.DeleteMPool(mp) })
			rows := vector.NewVec(types.T_uint64.ToType())
			b.Cleanup(func() { rows.Free(mp) })
			values := make([]uint64, 1000)
			for i := range values {
				values[i] = uint64(i)
			}
			want := 0
			for i := 0; i < objectio.BlockMaxRows; i++ {
				value := uint64(i / 4)
				if inverted && i == objectio.BlockMaxRows-1 {
					value = 0
				}
				require.NoError(b, vector.AppendFixed(rows, value, false, mp))
				if value < uint64(len(values)) {
					want++
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, search := orderedInSearchFuncs(values, true)
				for block := 0; block < 16; block++ {
					if got := len(search(rows)); got != want {
						b.Fatalf("matches %d want %d", got, want)
					}
				}
			}
		})
	}
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

// Include first-use construction as well as the amortized reader workload.
func BenchmarkUnsortedPKInReader(b *testing.B) {
	for _, count := range []int{4, 64, 1000, 10000, maxUnsortedINHashKeys, maxUnsortedINHashKeys + 1} {
		for _, blocks := range []int{1, 124} {
			b.Run(fmt.Sprintf("keys_%d/blocks_%d", count, blocks), func(b *testing.B) {
				mp := mpool.MustNewZero()
				b.Cleanup(func() { mpool.DeleteMPool(mp) })
				rows := vector.NewVec(types.T_uint64.ToType())
				b.Cleanup(func() { rows.Free(mp) })
				mix := func(x uint64) uint64 {
					x ^= x >> 30
					x *= 0xbf58476d1ce4e5b9
					x ^= x >> 27
					x *= 0x94d049bb133111eb
					return x ^ (x >> 31)
				}
				values := make([]uint64, count)
				for i := range values {
					values[i] = mix(uint64(i * 997))
				}
				slices.Sort(values)
				for i := 0; i < 8192; i++ {
					require.NoError(b, vector.AppendFixed(rows, mix(uint64(i)), false, mp))
				}
				want := (8192-1)/997 + 1
				if want > count {
					want = count
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, search := orderedInSearchFuncs(values, true)
					for j := 0; j < blocks; j++ {
						if got := len(search(rows)); got != want {
							b.Fatalf("matches %d want %d", got, want)
						}
					}
				}
			})
		}
	}
}

// Compare construction and repeated partial blocks around the row threshold.
func BenchmarkUnsortedPKInRowSize(b *testing.B) {
	for _, test := range []struct {
		name         string
		rows, blocks int
		constant     bool
	}{
		{"tiny", 16, 1, false},
		{"below threshold", objectio.BlockMaxRows/2 - 1, 1, false},
		{"at threshold", objectio.BlockMaxRows / 2, 1, false},
		{"partial block reused", objectio.BlockMaxRows - 1, 32, false},
		{"full block reused", objectio.BlockMaxRows, 32, false},
		{"constant logical block", objectio.BlockMaxRows, 1, true},
	} {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { mpool.DeleteMPool(mp) })
			var rows *vector.Vector
			if test.constant {
				var err error
				rows, err = vector.NewConstFixed(types.T_uint64.ToType(), uint64(42), test.rows, mp)
				if rows != nil {
					b.Cleanup(func() { rows.Free(mp) })
				}
				require.NoError(b, err)
			} else {
				rows = vector.NewVec(types.T_uint64.ToType())
				b.Cleanup(func() { rows.Free(mp) })
				for i := 0; i < test.rows; i++ {
					require.NoError(b, vector.AppendFixed(rows, uint64(2*(i%1000)+1), false, mp))
				}
			}
			values := make([]uint64, 1000)
			for i := range values {
				values[i] = uint64(2 * i)
			}
			want := 0
			if test.constant {
				want = 1
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, search := orderedInSearchFuncs(values, true)
				for j := 0; j < test.blocks; j++ {
					if got := len(search(rows)); got != want {
						b.Fatalf("matches %d want %d", got, want)
					}
				}
			}
		})
	}
}

func BenchmarkUnsortedPKInFloat(b *testing.B) {
	mp := mpool.MustNewZero()
	b.Cleanup(func() { mpool.DeleteMPool(mp) })
	needles := vector.NewVec(types.T_float64.ToType())
	b.Cleanup(func() { needles.Free(mp) })
	rows := vector.NewVec(types.T_float64.ToType())
	b.Cleanup(func() { rows.Free(mp) })
	for i := 0; i < 1000; i++ {
		require.NoError(b, vector.AppendFixed(needles, float64(2*i), false, mp))
	}
	needles.SetSorted(true)
	for i := 0; i < objectio.BlockMaxRows; i++ {
		require.NoError(b, vector.AppendFixed(rows, float64(2*(i%1000)+1), false, mp))
	}
	base := BasePKFilter{Valid: true, Op: function.IN, Oid: types.T_float64, Vec: needles}
	batch := containers.Vectors{*rows}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			filter, err := ConstructBlockPKFilter(true, base, nil)
			if err != nil {
				b.Fatal(err)
			}
			if filter.Cleanup != nil {
				defer filter.Cleanup()
			}
			if got := len(filter.DecideSearchFunc(true)(batch)); got != 0 {
				b.Fatalf("unexpected %d matches", got)
			}
		}()
	}
}
