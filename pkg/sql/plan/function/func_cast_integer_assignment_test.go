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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFloatIntegerAssignmentRounding(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		target types.T
		want   any
	}{
		{types.T_int8, []int8{2, 4, 0}}, {types.T_int16, []int16{2, 4, 0}},
		{types.T_int32, []int32{2, 4, 0}}, {types.T_int64, []int64{2, 4, 0}},
		{types.T_uint8, []uint8{2, 4, 0}}, {types.T_uint16, []uint16{2, 4, 0}},
		{types.T_uint32, []uint32{2, 4, 0}}, {types.T_uint64, []uint64{2, 4, 0}},
	} {
		t.Run(tc.target.String(), func(t *testing.T) {
			for _, src := range []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(), []float32{2.5, 3.5, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{2.5, 3.5, 0}, []bool{false, false, true}),
			} {
				inputs := []FunctionTestInput{src, NewFunctionTestInput(tc.target.ToType(), tc.want, nil)}
				expected := NewFunctionTestResult(tc.target.ToType(), false, tc.want, []bool{false, false, true})
				testCase := NewFunctionTestCase(proc, inputs, expected, NewAssignCast)
				ok, info := testCase.Run()
				require.True(t, ok, info)
				testCase = NewFunctionTestCase(proc, inputs, expected, NewAssignIgnoreCast)
				ok, info = testCase.Run()
				require.True(t, ok, info)
			}
		})
	}
}

func TestFloatIntegerAssignmentBounds(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name   string
		target types.T
		input  float64
		want   any
		fails  bool
	}{
		{"negative_even", types.T_int64, -2.5, []int64{-2}, false},
		{"negative_odd", types.T_int64, -3.5, []int64{-4}, false},
		{"signed_min_tie", types.T_int8, -128.5, []int8{-128}, false},
		{"signed_max_tie", types.T_int8, 127.5, []int8{0}, true},
		{"unsigned_max_tie", types.T_uint8, 255.5, []uint8{0}, true},
		{"unsigned_negative", types.T_uint64, -1, []uint64{0}, true},
		{"signed_2pow63", types.T_int64, math.Ldexp(1, 63), []int64{0}, true},
		{"signed_min", types.T_int64, -math.Ldexp(1, 63), []int64{math.MinInt64}, false},
		{"unsigned_2pow64", types.T_uint64, math.Ldexp(1, 64), []uint64{0}, true},
		{"unsigned_predecessor", types.T_uint64, math.Nextafter(math.Ldexp(1, 64), 0), []uint64{math.MaxUint64 - 2047}, false},
		{"nan", types.T_int64, math.NaN(), []int64{0}, true},
		{"infinity", types.T_int64, math.Inf(1), []int64{0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{tc.input}, nil),
				NewFunctionTestInput(tc.target.ToType(), tc.want, nil),
			}
			expected := NewFunctionTestResult(tc.target.ToType(), tc.fails, tc.want, nil)
			testCase := NewFunctionTestCase(proc, inputs, expected, NewAssignCast)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}
}
