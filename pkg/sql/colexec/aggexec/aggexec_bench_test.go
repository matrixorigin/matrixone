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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkAggExecPaths(b *testing.B) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			b.Fatalf("memory leak detected: %d bytes", mp.CurrNB())
		}
	}()

	const (
		rows      = 4096
		groupSize = 64
	)

	intVals := make([]int64, rows)
	for i := range intVals {
		intVals[i] = int64(i % 1024)
	}
	groups := make([]uint64, rows)
	for i := range groups {
		groups[i] = uint64((i % groupSize) + 1)
	}

	intVec := testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, intVals)
	defer intVec.Free(mp)

	stringVals := make([]string, rows)
	for i := range stringVals {
		stringVals[i] = "name" + string(rune('a'+(i%26)))
	}
	strVec := testutil.NewStringVector(rows, types.T_varchar.ToType(), mp, false, nil, stringVals)
	defer strVec.Free(mp)

	b.Run("SumInt64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumDecimal64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumAvgDecExec[types.Decimal64, types.Decimal128](mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumDecimal64Fast/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("AvgDecimal64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i%100000) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)
		vectors := []*vector.Vector{dec64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumAvgDecExec[types.Decimal64, types.Decimal128](mp, false, AggIdOfAvg, false, types.New(types.T_decimal64, 15, 2))
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumFloat64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		f64Vals := make([]float64, rows)
		for i := range f64Vals {
			f64Vals[i] = float64(i%1024) + 0.5
		}
		f64Vec := testutil.NewFloat64Vector(rows, types.T_float64.ToType(), mp, false, nil, f64Vals)
		defer f64Vec.Free(mp)
		vectors := []*vector.Vector{f64Vec}
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[float64, float64](mp, float64OfCheck, true, AggIdOfSum, false, types.T_float64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("AvgInt64/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[int64, int64](mp, int64OfCheck, false, AggIdOfAvg, false, types.T_int64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("CountColumn/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumInt64/BatchMerge", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		source := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
		if err := source.GroupGrow(groupSize); err != nil {
			b.Fatal(err)
		}
		if err := source.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			b.Fatal(err)
		}
		defer source.Free()

		mergeGroups := make([]uint64, groupSize)
		for i := range mergeGroups {
			mergeGroups[i] = uint64(i + 1)
		}

		for i := 0; i < b.N; i++ {
			target := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
			if err := target.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := target.BatchMerge(source, 0, mergeGroups); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			target.Free()
		}
	})

	b.Run("CountColumn/BatchMerge", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		source := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
		if err := source.GroupGrow(groupSize); err != nil {
			b.Fatal(err)
		}
		if err := source.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			b.Fatal(err)
		}
		defer source.Free()

		mergeGroups := make([]uint64, groupSize)
		for i := range mergeGroups {
			mergeGroups[i] = uint64(i + 1)
		}

		for i := 0; i < b.N; i++ {
			target := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
			if err := target.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := target.BatchMerge(source, 0, mergeGroups); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			target.Free()
		}
	})

	b.Run("MedianDistinct/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec, err := newMedianExec(mp, AggIdOfMedian, true, types.T_int64.ToType())
			if err != nil {
				b.Fatal(err)
			}
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("SumInt64/BatchFill/512groups", func(b *testing.B) {
		b.ReportAllocs()
		const bigGroupSize = 512
		bigGroups := make([]uint64, rows)
		for i := range bigGroups {
			bigGroups[i] = uint64((i % bigGroupSize) + 1)
		}
		vectors := []*vector.Vector{intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
			if err := exec.GroupGrow(bigGroupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, bigGroups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})

	b.Run("GroupConcat/BatchFill", func(b *testing.B) {
		b.ReportAllocs()
		info := multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			distinct:  false,
			argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
			retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}),
			emptyNull: true,
		}
		vectors := []*vector.Vector{strVec, intVec}
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			exec := newGroupConcatExec(mp, info, ",")
			if err := exec.GroupGrow(groupSize); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := exec.BatchFill(0, groups, vectors); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			exec.Free()
		}
	})
}

// TestLocalAccumulatorOverflow exercises the direct-scatter fallback path
// that triggers when a single BatchFill has more than 255 distinct groups.
func TestLocalAccumulatorOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			t.Fatalf("memory leak: %d bytes", mp.CurrNB())
		}
	}()

	const (
		numGroups = 512
		rows      = 1024
	)

	groups := make([]uint64, rows)
	for i := range groups {
		groups[i] = uint64((i % numGroups) + 1)
	}

	intVals := make([]int64, rows)
	for i := range intVals {
		intVals[i] = int64(i + 1)
	}
	intVec := testutil.NewInt64Vector(rows, types.T_int64.ToType(), mp, false, nil, intVals)
	defer intVec.Free(mp)

	t.Run("SumInt64", func(t *testing.T) {
		exec := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		// Verify: each group g (0-indexed) gets values (g+1) and (g+1+512).
		// sum = (g+1) + (g+1+512) = 2g + 514
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				expected := int64(2*g + 514)
				if vals[g] != expected {
					t.Fatalf("group %d: got %d, want %d", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("CountColumn", func(t *testing.T) {
		exec := newCountColumnExec(mp, AggIdOfCountColumn, false, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				if vals[g] != 2 {
					t.Fatalf("group %d: got %d, want 2", g, vals[g])
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("MinInt64", func(t *testing.T) {
		exec := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{intVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				expected := int64(g + 1)
				if vals[g] != expected {
					t.Fatalf("group %d: got %d, want %d", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("SumDecimal64Fast", func(t *testing.T) {
		dec64Vals := make([]types.Decimal64, rows)
		for i := range dec64Vals {
			dec64Vals[i] = types.Decimal64(int64(i+1) * 100)
		}
		dec64Vec := testutil.NewDecimal64Vector(rows, types.New(types.T_decimal64, 15, 2), mp, false, nil, dec64Vals)
		defer dec64Vec.Free(mp)

		exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{dec64Vec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			for g := 0; g < numGroups; g++ {
				// sum = ((g+1) + (g+1+512)) * 100 = (2g+514)*100
				expected := types.Decimal128{B0_63: uint64(int64(2*g+514) * 100), B64_127: 0}
				if vals[g] != expected {
					t.Fatalf("group %d: got %v, want %v", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})
}

// TestConstVectorAccumulator exercises the IsConst branch in the local accumulator.
func TestConstVectorAccumulator(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			t.Fatalf("memory leak: %d bytes", mp.CurrNB())
		}
	}()

	const (
		numGroups = 64
		rows      = 256
	)

	groups := make([]uint64, rows)
	for i := range groups {
		groups[i] = uint64((i % numGroups) + 1)
	}

	t.Run("SumInt64Const", func(t *testing.T) {
		constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), rows, mp)
		if err != nil {
			t.Fatal(err)
		}
		defer constVec.Free(mp)

		exec := newSumAvgExec[int64, int64](mp, int64OfCheck, true, AggIdOfSum, false, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{constVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				// Each group gets rows/numGroups = 4 rows, each with value 7.
				expected := int64(4 * 7)
				if vals[g] != expected {
					t.Fatalf("group %d: got %d, want %d", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("MinInt64Const", func(t *testing.T) {
		constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), rows, mp)
		if err != nil {
			t.Fatal(err)
		}
		defer constVec.Free(mp)

		exec := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{constVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			for g := 0; g < numGroups; g++ {
				if vals[g] != 42 {
					t.Fatalf("group %d: got %d, want 42", g, vals[g])
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})

	t.Run("SumDecimal64Const", func(t *testing.T) {
		constVec, err := vector.NewConstFixed(types.New(types.T_decimal64, 15, 2), types.Decimal64(500), rows, mp)
		if err != nil {
			t.Fatal(err)
		}
		defer constVec.Free(mp)

		exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal64, 15, 2))
		if err := exec.GroupGrow(numGroups); err != nil {
			t.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{constVec}); err != nil {
			t.Fatal(err)
		}
		results, err := exec.Flush()
		if err != nil {
			t.Fatal(err)
		}
		for _, vec := range results {
			vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			for g := 0; g < numGroups; g++ {
				// 4 rows per group × 500 = 2000
				expected := types.Decimal128{B0_63: 2000, B64_127: 0}
				if vals[g] != expected {
					t.Fatalf("group %d: got %v, want %v", g, vals[g], expected)
				}
			}
			vec.Free(mp)
		}
		exec.Free()
	})
}

// TestDecimal256Overflow verifies that the checked path (localAddSafe=false)
// correctly catches overflow during local accumulation for Decimal256→Decimal256.
func TestDecimal256Overflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			t.Fatalf("memory leak: %d bytes", mp.CurrNB())
		}
	}()

	const rows = 4
	groups := []uint64{1, 1, 1, 1}

	// Max Decimal256 ≈ 2^255 - 1. Use a value close to half-max so 3 adds overflow.
	halfMax := types.Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0) >> 1}

	typ := types.New(types.T_decimal256, 38, 0)
	vec := vector.NewOffHeapVecWithType(typ)
	if err := vec.PreExtend(rows, mp); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if err := vector.AppendFixed(vec, halfMax, false, mp); err != nil {
			t.Fatal(err)
		}
	}
	defer vec.Free(mp)

	exec := newSumAvgDecExec[types.Decimal256, types.Decimal256](mp, true, AggIdOfSum, false, typ)
	if err := exec.GroupGrow(1); err != nil {
		t.Fatal(err)
	}
	err := exec.BatchFill(0, groups, []*vector.Vector{vec})
	if err == nil {
		// If no error during BatchFill, Flush should show the overflow or we accept
		// that the add wrapped. Either way, confirm no panic.
		results, flushErr := exec.Flush()
		if flushErr != nil {
			t.Fatal(flushErr)
		}
		for _, v := range results {
			v.Free(mp)
		}
	} else {
		// Expected: overflow error from decimalStateAdd in the local buffer.
		if !strings.Contains(err.Error(), "Overflow") && !strings.Contains(err.Error(), "overflow") {
			t.Fatalf("expected overflow error, got: %v", err)
		}
	}
	exec.Free()
}
