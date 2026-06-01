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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimal128IntDivCorrectness(t *testing.T) {
	proc := testutil.NewProcess(t)

	typ := types.T_decimal128.ToType()

	testCases := []struct {
		info     string
		left     types.Decimal128
		right    types.Decimal128
		expected int64
	}{
		{"14 DIV 3 = 4", mustParseD128("14"), mustParseD128("3"), 4},
		{"12 DIV 3 = 4", mustParseD128("12"), mustParseD128("3"), 4},
		{"10 DIV 3 = 3", mustParseD128("10"), mustParseD128("3"), 3},
		{"5 DIV 3 = 1", mustParseD128("5"), mustParseD128("3"), 1},
		{"100 DIV 7 = 14", mustParseD128("100"), mustParseD128("7"), 14},
		{"1 DIV 3 = 0", mustParseD128("1"), mustParseD128("3"), 0},
		{"0 DIV 3 = 0", mustParseD128("0"), mustParseD128("3"), 0},
	}

	for _, tc := range testCases {
		tcc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(typ, []types.Decimal128{tc.left}, []bool{false}),
				NewFunctionTestInput(typ, []types.Decimal128{tc.right}, []bool{false}),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, []int64{tc.expected}, []bool{false}),
			integerDivFn,
		)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func TestDecimal128IntDivOverflowMessage(t *testing.T) {
	v1 := []types.Decimal128{mustParseD128("12345678909876543212345678909876543243")}
	v2 := []types.Decimal128{{B0_63: 4}}
	rs := make([]int64, 1)
	rsnull := nulls.NewWithSize(1)

	err := d128IntDiv(v1, v2, rs, 0, 0, rsnull, true)
	require.ErrorContains(t, err, "Decimal128 Div overflow: 12345678909876543212345678909876543243/4")
}

func TestDecimal128IntDivBigIntOverflowMessage(t *testing.T) {
	v1 := []types.Decimal128{mustParseD128("99999999999999999999")}
	v2 := []types.Decimal128{{B0_63: 1}}
	rs := make([]int64, 1)
	rsnull := nulls.NewWithSize(1)

	err := d128IntDiv(v1, v2, rs, 0, 0, rsnull, true)
	require.ErrorContains(t, err, "data out of range: data type BIGINT")
}

func TestDecimal128IntDivFallbackCoverage(t *testing.T) {
	var dst types.Decimal128
	require.False(t, d128IntDivInline(types.Decimal128{B64_127: 1 << 62}, 1, 0, 10, &dst))
	require.False(t, d128IntDivInline(types.Decimal128{B0_63: ^uint64(0)}, 1, 0, ^uint64(0), &dst))

	rsnull := nulls.NewWithSize(2)
	err := d128IntDivOne(types.Decimal128{B0_63: 1}, types.Decimal128{}, &dst, 0, rsnull, 1, false, 0, 0)
	require.NoError(t, err)
	require.True(t, rsnull.Contains(1))

	err = d128IntDivOne(types.Decimal128{B0_63: 1}, types.Decimal128{}, &dst, 0, rsnull, 0, true, 0, 0)
	require.Error(t, err)

	large := types.Decimal128{B64_127: 1}
	divisor := mustParseD128("100000000000000000000")
	err = d128IntDivOne(large, divisor, &dst, 20, nulls.NewWithSize(1), 0, true, 0, 20)
	require.NoError(t, err)
	require.Equal(t, large, dst)

	err = d128IntDivOne(types.Decimal128{B0_63: 10}, large, &dst, -20, nulls.NewWithSize(1), 0, true, 20, 0)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128{}, dst)

	err = d128IntDivOne(types.Decimal128{B0_63: 1}, types.Decimal128{B0_63: 1}, &dst, 39, nulls.NewWithSize(1), 0, true, 0, 39)
	require.ErrorContains(t, err, "Decimal128 IntDiv overflow")

	err = d128IntDivOne(types.Decimal128{B0_63: 1}, large, &dst, -100, nulls.NewWithSize(1), 0, true, 100, 0)
	require.ErrorContains(t, err, "Decimal128 IntDiv overflow")
}

func TestDecimalIntDivScaleAndFallbackBranches(t *testing.T) {
	t.Run("D128ScalarVectorFallback", func(t *testing.T) {
		v1 := []types.Decimal128{{B64_127: 1}}
		v2 := []types.Decimal128{{B0_63: 2}, {B0_63: 4}}
		rs := make([]int64, len(v2))
		require.Error(t, d128IntDiv(v1, v2, rs, 0, 0, nulls.NewWithSize(len(v2)), true))
	})

	t.Run("D128VectorScalarFallback", func(t *testing.T) {
		v1 := []types.Decimal128{{B64_127: 1}, {B64_127: 2}}
		v2 := []types.Decimal128{{B0_63: 2}}
		rs := make([]int64, len(v1))
		require.Error(t, d128IntDiv(v1, v2, rs, 0, 0, nulls.NewWithSize(len(v1)), true))
	})

	t.Run("D64ScalarBranches", func(t *testing.T) {
		v1 := []types.Decimal64{100}
		v2 := []types.Decimal64{3, types.Decimal64(^uint64(2))}
		rs := make([]int64, len(v2))
		require.NoError(t, d64IntDiv(v1, v2, rs, 0, 0, nulls.NewWithSize(len(v2)), true))
		require.Equal(t, []int64{33, -33}, rs)
	})

	t.Run("D256GenericScaleAdjust", func(t *testing.T) {
		x := types.Decimal256{B128_191: 5}
		y := types.Decimal256{B128_191: 2}
		rs := make([]int64, 1)
		require.NoError(t, d256IntDiv([]types.Decimal256{x}, []types.Decimal256{y}, rs, 0, 1, nulls.NewWithSize(1), true))
		require.Equal(t, int64(25), rs[0])

		require.NoError(t, d256IntDiv([]types.Decimal256{x}, []types.Decimal256{y}, rs, 1, 0, nulls.NewWithSize(1), true))
		require.Equal(t, int64(0), rs[0])
	})
}

func mustParseD128(s string) types.Decimal128 {
	d, err := types.ParseDecimal128(s, 38, 0)
	if err != nil {
		panic(err)
	}
	return d
}
