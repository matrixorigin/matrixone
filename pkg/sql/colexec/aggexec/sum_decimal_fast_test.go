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
