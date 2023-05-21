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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOperatorOpBitAndInt64Fn(t *testing.T) {
	// 1 & 2 = 0
	// -1 & 2 = 2
	// null & 2 = null
	tc := tcTemp{
		info: "& test",
		inputs: []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{0, 2, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess()
	fcTC := testutil.NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitAndInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestOperatorOpBitOrInt64Fn(t *testing.T) {
	// 1 | 2 = 3
	// -1 | 2 = -1
	// null | 2 = null
	tc := tcTemp{
		info: "| test",
		inputs: []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, -1, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess()
	fcTC := testutil.NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitOrInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestOperatorOpBitXorInt64Fn(t *testing.T) {
	// 1 ^ 2 = 3
	// -1 ^ 2 = -3
	// null ^ 2 = null
	tc := tcTemp{
		info: "^ test",
		inputs: []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 0}, []bool{false, false, true}),
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, 2}, []bool{false, false, false}),
		},
		expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{3, -3, 0}, []bool{false, false, true}),
	}

	proc := testutil.NewProcess()
	fcTC := testutil.NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitXorInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestOperatorOpBitRightShiftInt64Fn(t *testing.T) {
	// 1024 >> 2 = 256
	// -5 >> 2 = -2
	// 2 >> -2 = 0
	// null >> 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1024, -5, 2, 0}, []bool{false, false, false, true}),
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, -2, 2}, []bool{false, false, false, true}),
		},
		expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{256, -2, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess()
	fcTC := testutil.NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftRightInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info)
}

func TestOperatorOpBitLeftShiftInt64Fn(t *testing.T) {
	// -1 << 2 = 4
	// -1 << 2 = -4
	// 2 << -2 = 0
	// null << 2 = null
	tc := tcTemp{
		info: ">> test",
		inputs: []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, -1, 2, 0}, []bool{false, false, false, true}),
			testutil.NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{2, 2, -2, 2}, []bool{false, false, false, true}),
		},
		expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
			[]int64{4, -4, 0, 0}, []bool{false, false, false, true}),
	}

	proc := testutil.NewProcess()
	fcTC := testutil.NewFunctionTestCase(proc,
		tc.inputs, tc.expect, operatorOpBitShiftLeftInt64Fn)
	s, info := fcTC.Run()
	require.True(t, s, info)
}
