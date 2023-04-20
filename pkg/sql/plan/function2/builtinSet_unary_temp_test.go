// Copyright 2021 Matrix Origin
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

package function2

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

type tcTemp struct {
	info   string
	inputs []testutil.FunctionTestInput
	expect testutil.FunctionTestResult
}

func initAbsTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test abs int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-23, 9999999, -11, -99999, 9999999, -11, -99999, 9999999, -11, -99999, 9999999, -11, -99999},
					[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{23, 9999999, 11, 99999, 9999999, 11, 99999, 9999999, 11, 99999, 9999999, 11, 99999},
				[]bool{false, false, false, false, false, false, false, false, false, false, false, false, false}),
		},
	}
}

func TestCast(t *testing.T) {
	testCases := initAbsTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AbsInt64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func BenchmarkAbsInt64(b *testing.B) {
	testCases := initAbsTestCase()
	proc := testutil.NewProcess()

	b.StartTimer()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, AbsInt64)
		_ = fcTC.BenchMarkRun()
	}
	b.StopTimer()
}

func initHexStringTestCase() []tcTemp {
	//TODO: Pending multi-row
	return []tcTemp{
		{
			info: "test hex string",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "", "255", ""},
					[]bool{false, false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"61", "", "323535", ""},
				[]bool{false, false, false, true}),
		},
	}
}

func TestHexString(t *testing.T) {
	testCases := initHexStringTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, HexString)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initHexInt64TestCase() []tcTemp {
	//TODO: Pending multi-row
	return []tcTemp{
		{
			info: "test hex int64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{255, 231323423423421, 0},
					[]bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"FF", "D2632E7B3BBD", ""},
				[]bool{false, false, true}),
		},
	}
}

func TestHexInt64(t *testing.T) {
	testCases := initHexInt64TestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, HexInt64)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
