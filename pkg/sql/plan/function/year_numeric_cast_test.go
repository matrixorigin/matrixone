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

func TestYearCastNumericAndNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	nulls := []bool{false, true}
	year := types.T_year.ToType()
	for _, tc := range []struct {
		name   string
		target FunctionTestInput
		expect FunctionTestResult
	}{
		{"year", NewFunctionTestInput(year, []types.MoYear{}, nil), NewFunctionTestResult(year, false, []types.MoYear{2024, 0}, nulls)},
		{"integer", NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil), NewFunctionTestResult(types.T_int64.ToType(), false, []int64{2024, 0}, nulls)},
		{"float", NewFunctionTestInput(types.T_float64.ToType(), []float64{}, nil), NewFunctionTestResult(types.T_float64.ToType(), false, []float64{2024, 0}, nulls)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := NewFunctionTestInput(year, []types.MoYear{2024, 0}, nulls)
			test := NewFunctionTestCase(proc, []FunctionTestInput{input, tc.target}, tc.expect, NewCast)
			ok, info := test.RunAndFree()
			require.True(t, ok, info)
		})
	}
}
