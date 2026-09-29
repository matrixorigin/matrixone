// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

func TestSpecialIntegerKernelsSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	mask := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}}
	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		run    fEvalFn
		want   []string
	}{
		{"format numeric", []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{99, 1234, 99}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 2, 0}, nil),
		}, formatIntegerPrecision, []string{"", "1,234.00", ""}},
		{"format text locale", []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"x", "1234.5", "x"}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 2, 0}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"", "de_DE", ""}, nil),
		}, formatIntegerPrecision, []string{"", "1.234,50", ""}},
		{"makedate", []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 2024, 0}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 60, 0}, nil),
		}, makeDateInteger, []string{"", "2024-02-29", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := NewFunctionTestCase(proc, tc.inputs, NewFunctionTestResult(types.T_varchar.ToType(), false, tc.want, []bool{true, false, true}), tc.run).WithSelectList(mask)
			ok, info := test.Run()
			require.True(t, ok, info)
		})
	}
}
