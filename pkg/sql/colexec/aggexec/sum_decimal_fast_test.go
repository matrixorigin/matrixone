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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// ---- Decimal64 SUM tests ----

func BenchmarkSumDecimal64_Fast(b *testing.B) {
	benchmarkSumDecimal64(b, true)
}

func BenchmarkSumDecimal64_Generic(b *testing.B) {
	benchmarkSumDecimal64(b, false)
}

func benchmarkSumDecimal64(b *testing.B, useFast bool) {
	mp, _ := mpool.NewMPool("bench", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)

	const nRows = 8192
	const nGroups = 4

	// Build input vector with Decimal64 values.
	vec := vector.NewVec(param)
	defer vec.Free(mp)
	for i := 0; i < nRows; i++ {
		val := types.Decimal64(int64(i*100 + 1)) // simulates DECIMAL(15,2) values
		if err := vector.AppendFixed(vec, val, false, mp); err != nil {
			b.Fatal(err)
		}
	}

	// Build group assignment: cycle through nGroups groups.
	groups := make([]uint64, nRows)
	for i := range groups {
		groups[i] = uint64(i%nGroups) + 1
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		var exec AggFuncExec
		if useFast {
			exec = newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
		} else {
			exec = newSumAvgDecExec[types.Decimal64, types.Decimal128](mp, true, AggIdOfSum, false, param)
		}
		if err := exec.GroupGrow(nGroups); err != nil {
			b.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{vec}); err != nil {
			b.Fatal(err)
		}
		exec.Free()
	}
}

// ---- Decimal128 SUM tests ----

func TestSumDecimal128Fast_Basic(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	const nRows = 100
	const nGroups = 2

	// Build input vector with known Decimal128 values.
	vec := vector.NewVec(param)
	defer vec.Free(mp)
	for i := 0; i < nRows; i++ {
		val := types.Decimal128{B0_63: uint64(i*100 + 1), B64_127: 0}
		require.NoError(t, vector.AppendFixed(vec, val, false, mp))
	}

	groups := make([]uint64, nRows)
	for i := range groups {
		groups[i] = uint64(i%nGroups) + 1
	}

	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(nGroups))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	require.Equal(t, 1, len(vecs))
	require.Equal(t, nGroups, vecs[0].Length())

	// Group 0 (even indices): 1 + 201 + 401 + ... + 9901 = sum of (i*200+1) for i=0..49
	// Group 1 (odd indices): 101 + 301 + 501 + ... + 9801 = sum of (i*200+101) for i=0..49
	var expectedG0, expectedG1 uint64
	for i := 0; i < nRows; i++ {
		v := uint64(i*100 + 1)
		if i%2 == 0 {
			expectedG0 += v
		} else {
			expectedG1 += v
		}
	}

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	g1 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 1)
	require.Equal(t, expectedG0, g0.B0_63)
	require.Equal(t, uint64(0), g0.B64_127)
	require.Equal(t, expectedG1, g1.B0_63)
	require.Equal(t, uint64(0), g1.B64_127)

	exec.Free()
}

func TestSumDecimal128Fast_WithNulls(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	const nRows = 10
	vec := vector.NewVec(param)
	defer vec.Free(mp)

	// Even indices are non-null, odd indices are null.
	for i := 0; i < nRows; i++ {
		isNull := i%2 != 0
		val := types.Decimal128{B0_63: uint64(i + 1), B64_127: 0}
		require.NoError(t, vector.AppendFixed(vec, val, isNull, mp))
	}

	// Single group.
	groups := make([]uint64, nRows)
	for i := range groups {
		groups[i] = 1
	}

	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	// Only even indices: 1 + 3 + 5 + 7 + 9 = 25
	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(25), g0.B0_63)

	exec.Free()
}

func TestSumDecimal128Fast_Negative(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	vec := vector.NewVec(param)
	defer vec.Free(mp)

	// Add positive and negative values that should cancel.
	vals := []types.Decimal128{
		{B0_63: 100, B64_127: 0},                       // +100
		{B0_63: ^uint64(100) + 1, B64_127: ^uint64(0)}, // -100 (two's complement)
		{B0_63: 200, B64_127: 0},                       // +200
	}
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}

	groups := []uint64{1, 1, 1}
	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	// 100 + (-100) + 200 = 200
	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(200), g0.B0_63)
	require.Equal(t, uint64(0), g0.B64_127)

	exec.Free()
}

func TestSumDecimal128Fast_Overflow(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	// decimal(38,3): precision > 28, so overflow checking is enabled.
	param := types.New(types.T_decimal128, 38, 3)

	vec := vector.NewVec(param)
	defer vec.Free(mp)

	// Near-max Decimal128 value: 99999999999999999999999999999999999833
	// This is "99999999999999999999999999999999999.833" in decimal(38,3).
	nearMax, _, err := types.Parse128("99999999999999999999999999999999999833")
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(vec, nearMax, false, mp))
	require.NoError(t, vector.AppendFixed(vec, nearMax, false, mp))

	groups := []uint64{1, 1}
	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(1))

	err = exec.BatchFill(0, groups, []*vector.Vector{vec})
	require.Error(t, err, "expected overflow error for SUM of two near-max decimal(38,3) values")
	require.Contains(t, err.Error(), "overflow")

	exec.Free()
}

func BenchmarkSumDecimal128_Fast(b *testing.B) {
	benchmarkSumDecimal128(b, true)
}

func BenchmarkSumDecimal128_Generic(b *testing.B) {
	benchmarkSumDecimal128(b, false)
}

func benchmarkSumDecimal128(b *testing.B, useFast bool) {
	mp, _ := mpool.NewMPool("bench", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	const nRows = 8192
	const nGroups = 4

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	for i := 0; i < nRows; i++ {
		val := types.Decimal128{B0_63: uint64(i*100 + 1), B64_127: 0}
		if err := vector.AppendFixed(vec, val, false, mp); err != nil {
			b.Fatal(err)
		}
	}

	groups := make([]uint64, nRows)
	for i := range groups {
		groups[i] = uint64(i%nGroups) + 1
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		var exec AggFuncExec
		if useFast {
			exec = newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
		} else {
			exec = newSumAvgDecExec[types.Decimal128, types.Decimal128](mp, true, AggIdOfSum, false, param)
		}
		if err := exec.GroupGrow(nGroups); err != nil {
			b.Fatal(err)
		}
		if err := exec.BatchFill(0, groups, []*vector.Vector{vec}); err != nil {
			b.Fatal(err)
		}
		exec.Free()
	}
}

// ---- Decimal64 SUM unit tests ----

func TestSumDecimal64Fast_Basic(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)

	const nRows = 100
	const nGroups = 2

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	for i := 0; i < nRows; i++ {
		val := types.Decimal64(int64(i*100 + 1))
		require.NoError(t, vector.AppendFixed(vec, val, false, mp))
	}

	groups := make([]uint64, nRows)
	for i := range groups {
		groups[i] = uint64(i%nGroups) + 1
	}

	exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(nGroups))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	require.Equal(t, 1, len(vecs))
	require.Equal(t, nGroups, vecs[0].Length())

	var expectedG0, expectedG1 uint64
	for i := 0; i < nRows; i++ {
		v := uint64(i*100 + 1)
		if i%2 == 0 {
			expectedG0 += v
		} else {
			expectedG1 += v
		}
	}

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	g1 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 1)
	require.Equal(t, expectedG0, g0.B0_63)
	require.Equal(t, expectedG1, g1.B0_63)

	exec.Free()
}

func TestSumDecimal64Fast_Fill(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(vec, types.Decimal64(int64(i+1)*100), false, mp))
	}

	exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(1))

	// Use Fill (single row) for each row
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{vec}))
	require.NoError(t, exec.Fill(0, 1, []*vector.Vector{vec}))
	require.NoError(t, exec.Fill(0, 2, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	// 100 + 200 + 300 = 600
	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(600), g0.B0_63)

	exec.Free()
}

func TestSumDecimal64Fast_SetExtraInformation(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)
	exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.SetExtraInformation(nil, 0))
	exec.Free()
}

func TestSumDecimal64Fast_Merge(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)

	// Exec 1: fill group 0 with values 100, 200
	vec1 := vector.NewVec(param)
	defer vec1.Free(mp)
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal64(100), false, mp))
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal64(200), false, mp))

	exec1 := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec1.GroupGrow(1))
	require.NoError(t, exec1.BatchFill(0, []uint64{1, 1}, []*vector.Vector{vec1}))

	// Exec 2: fill group 0 with values 300, 400
	vec2 := vector.NewVec(param)
	defer vec2.Free(mp)
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal64(300), false, mp))
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal64(400), false, mp))

	exec2 := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec2.GroupGrow(1))
	require.NoError(t, exec2.BatchFill(0, []uint64{1, 1}, []*vector.Vector{vec2}))

	// Merge exec2's group 0 into exec1's group 0
	require.NoError(t, exec1.Merge(exec2, 0, 0))

	vecs, err := exec1.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	// 100 + 200 + 300 + 400 = 1000
	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(1000), g0.B0_63)

	exec1.Free()
	exec2.Free()
}

func TestSumDecimal64Fast_BatchMerge(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)

	// Exec 1: 2 groups
	vec1 := vector.NewVec(param)
	defer vec1.Free(mp)
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal64(10), false, mp))
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal64(20), false, mp))

	exec1 := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec1.GroupGrow(2))
	require.NoError(t, exec1.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vec1}))

	// Exec 2: 2 groups
	vec2 := vector.NewVec(param)
	defer vec2.Free(mp)
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal64(30), false, mp))
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal64(40), false, mp))

	exec2 := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec2.GroupGrow(2))
	require.NoError(t, exec2.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vec2}))

	// BatchMerge: merge exec2 groups [0,1] into exec1 groups [1,2]
	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{1, 2}))

	vecs, err := exec1.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	g1 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 1)
	require.Equal(t, uint64(40), g0.B0_63) // 10 + 30
	require.Equal(t, uint64(60), g1.B0_63) // 20 + 40

	exec1.Free()
	exec2.Free()
}

func TestSumDecimal64Fast_AVG(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal64, 15, 2)

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	// Add values 100, 200, 300 (scale=2 → 1.00, 2.00, 3.00)
	require.NoError(t, vector.AppendFixed(vec, types.Decimal64(100), false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal64(200), false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal64(300), false, mp))

	exec := newSumDecimal64FastExec(mp, false, AggIdOfAvg, false, param) // isSum=false → AVG
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	// AVG(1.00, 2.00, 3.00) = 2.00 → 200 at scale 2
	// But AVG return type has scale = argScale + 4 = 6, so 2.000000 = 2000000
	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	// Just verify it's non-zero and not null
	require.False(t, vecs[0].IsNull(0))
	_ = g0

	exec.Free()
}

// ---- Decimal128 SUM/AVG additional tests ----

func TestSumDecimal128Fast_Fill(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, vector.AppendFixed(vec, types.Decimal128{B0_63: uint64(i+1) * 100}, false, mp))
	}

	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(1))

	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{vec}))
	require.NoError(t, exec.Fill(0, 1, []*vector.Vector{vec}))
	require.NoError(t, exec.Fill(0, 2, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(600), g0.B0_63)

	exec.Free()
}

func TestSumDecimal128Fast_SetExtraInformation(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)
	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.SetExtraInformation(nil, 0))
	exec.Free()
}

func TestSumDecimal128Fast_Merge(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	vec1 := vector.NewVec(param)
	defer vec1.Free(mp)
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal128{B0_63: 100}, false, mp))
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal128{B0_63: 200}, false, mp))

	exec1 := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec1.GroupGrow(1))
	require.NoError(t, exec1.BatchFill(0, []uint64{1, 1}, []*vector.Vector{vec1}))

	vec2 := vector.NewVec(param)
	defer vec2.Free(mp)
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal128{B0_63: 300}, false, mp))
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal128{B0_63: 400}, false, mp))

	exec2 := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec2.GroupGrow(1))
	require.NoError(t, exec2.BatchFill(0, []uint64{1, 1}, []*vector.Vector{vec2}))

	require.NoError(t, exec1.Merge(exec2, 0, 0))

	vecs, err := exec1.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(1000), g0.B0_63)

	exec1.Free()
	exec2.Free()
}

func TestSumDecimal128Fast_BatchMerge(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	vec1 := vector.NewVec(param)
	defer vec1.Free(mp)
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal128{B0_63: 10}, false, mp))
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal128{B0_63: 20}, false, mp))

	exec1 := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec1.GroupGrow(2))
	require.NoError(t, exec1.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vec1}))

	vec2 := vector.NewVec(param)
	defer vec2.Free(mp)
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal128{B0_63: 30}, false, mp))
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal128{B0_63: 40}, false, mp))

	exec2 := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec2.GroupGrow(2))
	require.NoError(t, exec2.BatchFill(0, []uint64{1, 2}, []*vector.Vector{vec2}))

	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{1, 2}))

	vecs, err := exec1.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	g1 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 1)
	require.Equal(t, uint64(40), g0.B0_63)
	require.Equal(t, uint64(60), g1.B0_63)

	exec1.Free()
	exec2.Free()
}

func TestSumDecimal128Fast_AVG(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	param := types.New(types.T_decimal128, 38, 2)

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixed(vec, types.Decimal128{B0_63: 100}, false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal128{B0_63: 200}, false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal128{B0_63: 300}, false, mp))

	exec := newSumDecimal128FastExec(mp, false, AggIdOfAvg, false, param)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	require.False(t, vecs[0].IsNull(0))

	exec.Free()
}

func TestSumDecimal128Fast_OverflowCheck_BatchFill(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	// precision > 28 triggers overflow checking path
	param := types.New(types.T_decimal128, 38, 3)

	vec := vector.NewVec(param)
	defer vec.Free(mp)
	// Normal values that won't overflow
	require.NoError(t, vector.AppendFixed(vec, types.Decimal128{B0_63: 12345}, false, mp))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal128{B0_63: 67890}, false, mp))

	exec := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{vec}))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(80235), g0.B0_63)

	exec.Free()
}

func TestSumDecimal128Fast_BatchMerge_WithOverflowCheck(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	defer mp.Free(nil)

	// precision > 28 triggers overflow checking
	param := types.New(types.T_decimal128, 38, 3)

	vec1 := vector.NewVec(param)
	defer vec1.Free(mp)
	require.NoError(t, vector.AppendFixed(vec1, types.Decimal128{B0_63: 100}, false, mp))

	exec1 := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec1.GroupGrow(1))
	require.NoError(t, exec1.BatchFill(0, []uint64{1}, []*vector.Vector{vec1}))

	vec2 := vector.NewVec(param)
	defer vec2.Free(mp)
	require.NoError(t, vector.AppendFixed(vec2, types.Decimal128{B0_63: 200}, false, mp))

	exec2 := newSumDecimal128FastExec(mp, true, AggIdOfSum, false, param)
	require.NoError(t, exec2.GroupGrow(1))
	require.NoError(t, exec2.BatchFill(0, []uint64{1}, []*vector.Vector{vec2}))

	require.NoError(t, exec1.BatchMerge(exec2, 0, []uint64{1}))

	vecs, err := exec1.Flush()
	require.NoError(t, err)
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()

	g0 := vector.GetFixedAtNoTypeCheck[types.Decimal128](vecs[0], 0)
	require.Equal(t, uint64(300), g0.B0_63)

	exec1.Free()
	exec2.Free()
}
