// Copyright 2023 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_getSubstring(t *testing.T) {
	tests := []struct {
		name  string
		str   string
		start int
		want  string
	}{
		{
			name:  "test01",
			str:   "asdadfd这个文档dsfsssss",
			start: 8,
			want:  "个文档dsfsssss",
		},
		{
			name:  "test02",
			str:   "asdadfd这个文档dsfsssss",
			start: 4,
			want:  "dfd这个文档dsfsssss",
		},
		{
			name:  "test03",
			str:   "asdadfd这个文档dsfsssss",
			start: 0,
			want:  "asdadfd这个文档dsfsssss",
		},
		{
			name:  "test04",
			str:   "asdadfd这个文档dsfsssss",
			start: 40,
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getSubstring(tt.str, tt.start), "getSubstring(%v, %v)", tt.str, tt.start)
		})
	}
}

func initLocateTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ghi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{7},
				[]bool{false}),
		},
		{
			info: "2",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"gxhi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ghi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ghi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{7},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ghi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{6},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{7},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ghi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-6},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"ghi"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{8},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"xxx"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{8},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
		{
			info: "3",
			typ:  types.T_varchar,
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefghijklmn"},
					[]bool{false}),
				testutil.NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0},
				[]bool{false}),
		},
	}
}

func TestLocate(t *testing.T) {
	testCases := initLocateTestCase()

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		var fcTC testutil.FunctionTestCase
		switch tc.info {
		case "2":
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, buildInLocate2Args)
		case "3":
			fcTC = testutil.NewFunctionTestCase(proc,
				tc.inputs, tc.expect, buildInLocate3Args)
		}
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
