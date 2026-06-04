// Copyright 2021 - 2022 Matrix Origin
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

// issue #24565: a CASE/IFF whose branches need more than 38 digits is promoted
// to decimal256; comparing such a value with a decimal128 must work instead of
// failing with "bad value [DECIMAL256 DECIMAL128]".

func Test_FixedTypeCastRule1_Decimal256Promotion(t *testing.T) {
	// decimal256 vs decimal128 -> both promoted to decimal256, each keeps scale/width.
	has, t1, t2 := fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.New(types.T_decimal128, 38, 20),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, types.T_decimal256, t2.Oid)
	require.Equal(t, int32(20), t1.Scale)
	require.Equal(t, int32(58), t1.Width)
	require.Equal(t, int32(20), t2.Scale)
	require.Equal(t, int32(38), t2.Width)

	// decimal128 vs decimal256 (reversed) -> still both decimal256.
	has, t1, t2 = fixedTypeCastRule1(
		types.New(types.T_decimal128, 38, 2),
		types.New(types.T_decimal256, 58, 20),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, types.T_decimal256, t2.Oid)

	// decimal256 vs decimal256 -> both decimal256.
	has, t1, t2 = fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.New(types.T_decimal256, 40, 5),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, types.T_decimal256, t2.Oid)

	// decimal256 vs non-decimal must NOT be handled by the promotion shortcut.
	has, _, _ = fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.T_int64.ToType(),
	)
	require.False(t, has)
}

func Test_CompareOperatorSupports_Decimal256(t *testing.T) {
	d256 := types.New(types.T_decimal256, 58, 20)
	require.True(t, equalAndNotEqualOperatorSupports(d256, d256))
	require.True(t, otherCompareOperatorSupports(d256, d256))
}

func Test_EqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)

	// equal, same scale
	tc := tcTemp{
		info: "decimal256 = decimal256 (same scale)",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(6)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(7)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_EqualFn_Decimal256_DifferentScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	// 5 (scale 0) compared to 500 (scale 2) == 5.00 -> equal.
	tc := tcTemp{
		info: "decimal256 = decimal256 (different scale)",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5)}, []bool{false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 2),
				[]types.Decimal256{types.Decimal256FromInt64(500)}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true}, []bool{false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_GreatThanFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 > decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(1)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(1), types.Decimal256FromInt64(5)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatThanFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_NotEqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 != decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(6)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(7)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, notEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_NullSafeEqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 <=> decimal256 with nulls",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(0)},
				[]bool{false, true}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(0)},
				[]bool{false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}
