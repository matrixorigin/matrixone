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

func TestHnswCdcUpdateFn(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "nargs invalid",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "dbname null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "table name null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "dimension null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{0}, []bool{true}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "cdc json null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "cdc json invalid",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{"{..."}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, hnswCdcUpdate)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}
