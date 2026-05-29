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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimal128IntDivCorrectness(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal128, 38, 0)

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
		{"-14 DIV 3 = -4", mustParseD128("-14"), mustParseD128("3"), -4},
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
	_, err := decimal128IntDivToInt64(mustParseD128("12345678909876543212345678909876543243"), types.Decimal128{B0_63: 4}, 0, 0)
	require.ErrorContains(t, err, "data out of range: data type BIGINT")
}

func mustParseD128(s string) types.Decimal128 {
	d, err := types.ParseDecimal128(s, 38, 0)
	if err != nil {
		panic(err)
	}
	return d
}
