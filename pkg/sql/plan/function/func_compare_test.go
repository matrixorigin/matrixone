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
	"math"
	"math/big"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOperatorOpBitAndInt64Fn(t *testing.T) {
	// 1 & 2 = 0
	// -1 & 2 = 2
	// null & 2 = null
	tc := tcTemp{
		info: "& test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{0, 2, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitAndInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitOrInt64Fn(t *testing.T) {
	// 1 | 2 = 3
	// -1 | 2 = -1
	// null | 2 = null
	tc := tcTemp{
		info: "| test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, -1, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitOrInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitXorInt64Fn(t *testing.T) {
	// 1 ^ 2 = 3
	// -1 ^ 2 = -3
	// null ^ 2 = null
	tc := tcTemp{
		info: "^ test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, -3, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitXorInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitRightShiftInt64Fn(t *testing.T) {
	// 1024 >> 2 = 256
	// -5 >> 2 = -2
	// 2 >> -2 = 0
	// null >> 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1024, -5, 2, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, -2, 2}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{256, -2, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftRightInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestOperatorOpBitLeftShiftInt64Fn(t *testing.T) {
	// -1 << 2 = 4
	// -1 << 2 = -4
	// 2 << -2 = 0
	// null << 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 2, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, -2, 2}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{4, -4, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftLeftInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFnTypeMismatch(t *testing.T) {
	tc := tcTemp{
		info: "<=> mixed numeric test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 1, 0, 0}, []bool{false, false, true, true}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1, 2, 0, 0}, []bool{false, false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestEqualFnTypeMismatchLargeIntegers(t *testing.T) {
	tc := tcTemp{
		info: "= mixed large integer test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{9007199254740993, 9007199254740993}, []bool{false, false}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{9007199254740992, 9007199254740993}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true}, []bool{false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFnTypeMismatchLargeIntegers(t *testing.T) {
	tc := tcTemp{
		info: "<=> mixed large integer test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{9007199254740993, 9007199254740993}, []bool{false, false}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{9007199254740992, 9007199254740993}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true}, []bool{false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestEqualFnTypeMismatchDecimalAndFloat(t *testing.T) {
	tc := tcTemp{
		info: "= mixed decimal/float test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal64, 10, 1),
				[]types.Decimal64{1, 2}, []bool{false, false}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{0.1, 0.3}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestCompareFnTypeMismatchSpecialFloats(t *testing.T) {
	proc := testutil.NewProcess(t)

	eqTC := tcTemp{
		info: "= mixed special float test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 1, 1}, []bool{false, false, false}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{math.NaN(), math.Inf(1), math.Inf(-1)}, []bool{false, false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false, false}, []bool{false, false, false}),
	}
	fcTC := NewFunctionTestCase(proc, eqTC.inputs, eqTC.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, eqTC.info)

	neTC := tcTemp{
		info:   "!= mixed special float test",
		inputs: eqTC.inputs,
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true, true}, []bool{false, false, false}),
	}
	fcTC = NewFunctionTestCase(proc, neTC.inputs, neTC.expect, notEqualFn)
	s, info = fcTC.Run()
	require.True(t, s, info, neTC.info)
}

func TestNullSafeEqualFnSameTypeInt64(t *testing.T) {
	// Covers the same-type fast path for <=> on signed integers.
	// Row 2 and 3 exercise the (null, null) -> true and (null, non-null) -> false branches.
	tc := tcTemp{
		info: "<=> same-type int64",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 2, 0, 0}, []bool{false, false, true, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 3, 0, 5}, []bool{false, false, true, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, true, false}, []bool{false, false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFnSameTypeString(t *testing.T) {
	// Covers opBinaryBytesBytesToFixedNullSafe for varchar.
	tc := tcTemp{
		info: "<=> same-type varchar",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "b", "", ""}, []bool{false, false, true, true}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "c", "", "x"}, []bool{false, false, true, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, true, false}, []bool{false, false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFnSameTypeBool(t *testing.T) {
	tc := tcTemp{
		info: "<=> same-type bool",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, false, false}, []bool{false, false, true}),
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, true, false}, []bool{false, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, true}, []bool{false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFnMixedBothNullRow(t *testing.T) {
	// Exercises the non-const "both operands null" row branch in the exact-rational null-safe path.
	tc := tcTemp{
		info: "<=> mixed numeric both-null row",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{0, 5, 5}, []bool{true, false, false}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{0, 0, 5}, []bool{true, true, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, true}, []bool{false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestNullSafeEqualFnMixedFloatNaN(t *testing.T) {
	// NaN is never equal to anything under <=>, even to itself, because
	// compareApproximateNumericValues reports (0, false) for NaN and cmpFn requires comparable=true.
	tc := tcTemp{
		info: "<=> mixed float NaN",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 0}, []bool{false, true}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{math.NaN(), math.NaN()}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false}, []bool{false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func TestCompareFnTypeMismatchLargeIntOrdering(t *testing.T) {
	// 2^53 + 1 vs 2^53: equality test already covered elsewhere; here we cover
	// ordering operators through the exact-rational path where float64 would lose precision.
	proc := testutil.NewProcess(t)

	ltTC := tcTemp{
		info: "< mixed large int",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{9007199254740993, 9007199254740993}, []bool{false, false}),
			NewFunctionTestInput(types.T_uint64.ToType(),
				[]uint64{9007199254740992, 9007199254740994}, []bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, ltTC.inputs, ltTC.expect, lessThanFn)
	s, info := fcTC.Run()
	require.True(t, s, info, ltTC.info)

	gtTC := tcTemp{
		info:   "> mixed large int",
		inputs: ltTC.inputs,
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC = NewFunctionTestCase(proc, gtTC.inputs, gtTC.expect, greatThanFn)
	s, info = fcTC.Run()
	require.True(t, s, info, gtTC.info)

	leTC := tcTemp{
		info:   "<= mixed large int",
		inputs: ltTC.inputs,
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true}, []bool{false, false}),
	}
	fcTC = NewFunctionTestCase(proc, leTC.inputs, leTC.expect, lessEqualFn)
	s, info = fcTC.Run()
	require.True(t, s, info, leTC.info)

	geTC := tcTemp{
		info:   ">= mixed large int",
		inputs: ltTC.inputs,
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC = NewFunctionTestCase(proc, geTC.inputs, geTC.expect, greatEqualFn)
	s, info = fcTC.Run()
	require.True(t, s, info, geTC.info)
}

func TestCompareNumericValuesSpecialKinds(t *testing.T) {
	// Exercises compareNumericValues directly for infinities and NaN, since the
	// fast-path/slow-path plumbing can mask whether each branch is reached.
	posInf := numericCompareValue{kind: numericComparePosInf}
	negInf := numericCompareValue{kind: numericCompareNegInf}
	nan := numericCompareValue{kind: numericCompareNaN}
	finite := func(v int64) numericCompareValue {
		return numericCompareValue{kind: numericCompareFinite, rat: new(big.Rat).SetInt64(v)}
	}

	cases := []struct {
		name     string
		a, b     numericCompareValue
		wantCmp  int
		wantComp bool
	}{
		{"+inf vs +inf", posInf, posInf, 0, true},
		{"+inf vs finite", posInf, finite(0), 1, true},
		{"-inf vs -inf", negInf, negInf, 0, true},
		{"-inf vs finite", negInf, finite(0), -1, true},
		{"finite vs +inf", finite(0), posInf, -1, true},
		{"finite vs -inf", finite(0), negInf, 1, true},
		{"NaN vs finite", nan, finite(0), 0, false},
		{"finite vs NaN", finite(0), nan, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCmp, gotComp := compareNumericValues(tc.a, tc.b)
			require.Equal(t, tc.wantComp, gotComp)
			if tc.wantComp {
				require.Equal(t, tc.wantCmp, gotCmp)
			}
		})
	}
}
