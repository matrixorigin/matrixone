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

func TestNullSafeEqualFn(t *testing.T) {
	// 1 <=> 1 = true
	// 1 <=> 0 = false
	// 1 <=> null = false
	// null <=> 1 = false
	// null <=> null = true
	tcInt64 := tcTemp{
		info: "<=> int64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 1, 1, 0, 0}, []bool{false, false, false, true, true}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 0, 0, 1, 0}, []bool{false, false, true, false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, false, true}, []bool{false, false, false, false, false}),
	}

	proc := testutil.NewProcess(t)
	fcTCInt64 := NewFunctionTestCase(proc,
		tcInt64.inputs, tcInt64.expect, nullSafeEqualFn)
	s, info := fcTCInt64.Run()
	require.True(t, s, info, tcInt64.info)

	// Float64 Test
	// 1.1 <=> 1.1 = true
	// 1.1 <=> 0.0 = false
	// 1.1 <=> null = false
	// null <=> null = true
	tcFloat := tcTemp{
		info: "<=> float64 test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.1, 1.1, 1.1, 0.0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.1, 0.0, 0.0, 0.0}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCFloat := NewFunctionTestCase(proc,
		tcFloat.inputs, tcFloat.expect, nullSafeEqualFn)
	s, info = fcTCFloat.Run()
	require.True(t, s, info, tcFloat.info)

	// Varchar Test
	// "a" <=> "a" = true
	// "a" <=> "b" = false
	// "a" <=> null = false
	// null <=> null = true
	tcStr := tcTemp{
		info: "<=> varchar test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "a", "a", ""}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a", "b", "", ""}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCStr := NewFunctionTestCase(proc,
		tcStr.inputs, tcStr.expect, nullSafeEqualFn)
	s, info = fcTCStr.Run()
	require.True(t, s, info, tcStr.info)

	// Bool Test
	// true <=> true = true
	// true <=> false = false
	// true <=> null = false
	// null <=> null = true
	tcBool := tcTemp{
		info: "<=> bool test",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, true, true, false}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, false, false, false}, []bool{false, false, true, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true}, []bool{false, false, false, false}),
	}
	fcTCBool := NewFunctionTestCase(proc,
		tcBool.inputs, tcBool.expect, nullSafeEqualFn)
	s, info = fcTCBool.Run()
	require.True(t, s, info, tcBool.info)
}
