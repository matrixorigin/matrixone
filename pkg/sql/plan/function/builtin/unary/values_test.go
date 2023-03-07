// Copyright 2022 Matrix Origin
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

package unary

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestValues(t *testing.T) {
	type tcTemp struct {
		info   string
		inputs []testutil.FunctionTestInput
		expect testutil.FunctionTestResult
	}

	testCases := []tcTemp{
		{
			info: "values(col_int8)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-23}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{-23}, []bool{false}),
		},
		{
			info: "values(col_uint8)",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{23, 24, 25}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{23, 24, 25}, []bool{false}),
		},
	}

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Values)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
