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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// #33228: YEAR values must not wrap when narrowed to TINYINT.
func TestYearToNarrowIntegerDoesNotWrap33228(t *testing.T) {
	year := types.T_year.ToType()

	t.Run("strict rejects out of range", func(t *testing.T) {
		for _, target := range []FunctionTestInput{
			NewFunctionTestInput(types.T_int8.ToType(), []int8{}, nil),
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, nil),
		} {
			proc := testutil.NewProcess(t)
			input := NewFunctionTestInput(year, []types.MoYear{2024}, nil)
			expect := NewFunctionTestResult(target.typ, true, nil, nil)
			ok, info := runYearCast33228(proc, []FunctionTestInput{input, target}, expect, NewStrictCast)
			require.True(t, ok, info)
			ok, info = runYearCast33228(proc, []FunctionTestInput{input, target}, expect, NewCast)
			require.True(t, ok, info)
		}
	})

	t.Run("in range values still convert", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		input := NewFunctionTestInput(year, []types.MoYear{0, 0}, []bool{false, true})
		target := NewFunctionTestInput(types.T_int8.ToType(), []int8{}, nil)
		expect := NewFunctionTestResult(types.T_int8.ToType(), false, []int8{0, 0}, []bool{false, true})
		ok, info := runYearCast33228(proc, []FunctionTestInput{input, target}, expect, NewStrictCast)
		require.True(t, ok, info)

		input = NewFunctionTestInput(year, []types.MoYear{2024, 1901}, nil)
		target = NewFunctionTestInput(types.T_int16.ToType(), []int16{}, nil)
		expect = NewFunctionTestResult(types.T_int16.ToType(), false, []int16{2024, 1901}, nil)
		ok, info = runYearCast33228(proc, []FunctionTestInput{input, target}, expect, NewStrictCast)
		require.True(t, ok, info)
	})

	t.Run("ignore clips with warning", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		session := &numericWarningSession{}
		proc.Session = session
		input := NewFunctionTestInput(year, []types.MoYear{2024, 0, 100}, nil)
		target := NewFunctionTestInput(types.T_int8.ToType(), []int8{}, nil)
		expect := NewFunctionTestResult(types.T_int8.ToType(), false, []int8{127, 0, 100}, nil)
		ok, info := runYearCast33228(proc, []FunctionTestInput{input, target}, expect, NewAssignIgnoreCast)
		require.True(t, ok, info)

		target = NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, nil)
		expectU := NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{255, 0, 100}, nil)
		ok, info = runYearCast33228(proc, []FunctionTestInput{input, target}, expectU, NewAssignIgnoreCast)
		require.True(t, ok, info)

		require.Len(t, session.warnings, 2)
		for _, w := range session.warnings {
			require.Equal(t, uint16(moerr.ER_WARN_DATA_OUT_OF_RANGE), w.code)
		}
	})
}

func runYearCast33228(proc *process.Process, inputs []FunctionTestInput, expect FunctionTestResult,
	fn func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error) (bool, string) {
	tc := NewFunctionTestCase(proc, inputs, expect, fn)
	return tc.RunAndFree()
}
