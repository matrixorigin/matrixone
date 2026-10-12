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

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Test_RoundUint64 tests ROUND function for uint64
// This complex function handles different precision levels and calls multiple helper functions
func Test_RoundUint64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	// Round to 0 decimal places (no change for integers)
	{
		tc := tcTemp{
			info: "select round(uint64_col, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{123, 456, 789, 1000}, []bool{false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{123, 456, 789, 1000}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundUint64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Round to negative decimal places (round to tens)
	{
		tc := tcTemp{
			info: "select round(uint64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{123, 456, 785}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				// 123->120, 456->460, 785->790
				[]uint64{120, 460, 790}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundUint64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Round to hundreds
	{
		tc := tcTemp{
			info: "select round(uint64_col, -2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1004, 1995}, []bool{false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-2}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				// 1004->1000, 1995->2000
				[]uint64{1000, 2000}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundUint64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_RoundInt64 tests ROUND function for int64
func Test_RoundInt64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	{
		tc := tcTemp{
			info: "select round(int64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{123, -456, 785, -1, -4, -5, -6, -10, -11, -14, -15, -16, 0}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				// Exact multiples stay fixed and signed ties round away from zero.
				[]int64{120, -460, 790, 0, 0, -10, -10, -10, -10, -10, -20, -20, 0}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Round to hundreds
	{
		tc := tcTemp{
			info: "select round(int64_col, -2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1004, 1995, 1050}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-2}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				// -1004->-1000, 1995->2000, 1050->1100
				[]int64{-1000, 2000, 1100}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Round to 0 decimal places
	{
		tc := tcTemp{
			info: "select round(int64_col, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, -200, 0}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{100, -200, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

func TestRoundInt64Boundaries(t *testing.T) {
	for _, tc := range []struct {
		name                string
		value, digits, want int64
		overflow            bool
	}{
		{"minimum representable coarse result", math.MinInt64, -18, -9000000000000000000, false},
		{"minimum rounding overflow", math.MinInt64, -1, 0, true},
		{"maximum rounding overflow", math.MaxInt64, -1, 0, true},
		{"positive nineteen below half", 4999999999999999999, -19, 0, false},
		{"negative nineteen below half", -4999999999999999999, -19, 0, false},
		{"positive nineteen tie", 5000000000000000000, -19, 0, true},
		{"negative nineteen tie", -5000000000000000000, -19, 0, true},
		{"maximum nineteen overflow", math.MaxInt64, -19, 0, true},
		{"minimum nineteen overflow", math.MinInt64, -19, 0, true},
		{"minimum twenty", math.MinInt64, -20, 0, false},
		{"maximum twenty", math.MaxInt64, -20, 0, false},
		{"minimum digits", math.MinInt64, math.MinInt64, 0, false},
		{"maximum digits identity", math.MinInt64, math.MaxInt64, math.MinInt64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result int64
			var recovered any
			func() { defer func() { recovered = recover() }(); result = roundInt64(tc.value, tc.digits) }()
			if !tc.overflow {
				require.Nil(t, recovered, "representable ROUND must succeed")
				require.Equal(t, tc.want, result)
				return
			}
			err, ok := recovered.(error)
			require.True(t, ok, "expected a typed overflow, got %v", recovered)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
		})
	}
}

func TestRoundInt64DynamicPrecisionAndNull(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	values := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{math.MinInt64}, nil)
	digits := NewFunctionTestInput(types.T_int64.ToType(), []int64{0, -18, math.MinInt64, math.MaxInt64, -1}, []bool{false, false, false, false, true})
	expected := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{math.MinInt64, -9000000000000000000, 0, math.MinInt64, 0}, []bool{false, false, false, false, true})
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, RoundInt64)
	defer tc.Free()
	tc.fnLength = tc.parameters[1].Length()
	tc.parameters[0].SetLength(tc.fnLength)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

// Test_RoundFloat64 tests ROUND function for float64
// This is particularly complex as it handles both positive and negative precision
func Test_RoundFloat64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	// Round to 0 decimal places
	{
		tc := tcTemp{
			info: "select round(float64_col, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.4, 1.5, 1.6, -1.4, -1.5, -1.6}, []bool{false, false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.0, 2.0, 2.0, -1.0, -2.0, -2.0}, []bool{false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Round to positive decimal places
	{
		tc := tcTemp{
			info: "select round(float64_col, 2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.234, 1.235, 1.236, -1.234, -1.235}, []bool{false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{2}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.23, 1.24, 1.24, -1.23, -1.24}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Round to negative decimal places (uses banker's rounding - RoundToEven)
	{
		tc := tcTemp{
			info: "select round(float64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{14.0, 15.0, 16.0, 24.0, 26.0}, []bool{false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				// 14->10, 15->20, 16->20, 24->20, 26->30 (RoundToEven: 1.5->2, 2.5->2)
				[]float64{10.0, 20.0, 20.0, 20.0, 30.0}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, RoundFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_CeilFloat64 tests CEIL function for float64
func Test_CeilFloat64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	// Ceil with 0 decimal places
	{
		tc := tcTemp{
			info: "select ceil(float64_col, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.1, 1.9, -1.1, -1.9, 0.0, 5.5}, []bool{false, false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2.0, 2.0, -1.0, -1.0, 0.0, 6.0}, []bool{false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CeilFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Ceil with positive decimal places
	{
		tc := tcTemp{
			info: "select ceil(float64_col, 1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.12, 1.19, -1.12, -1.19}, []bool{false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{1}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.2, 1.2, -1.1, -1.1}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CeilFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_FloorFloat64 tests FLOOR function for float64
func Test_FloorFloat64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	// Floor with 0 decimal places
	{
		tc := tcTemp{
			info: "select floor(float64_col, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.1, 1.9, -1.1, -1.9, 0.0, 5.5}, []bool{false, false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.0, 1.0, -2.0, -2.0, 0.0, 5.0}, []bool{false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Floor with positive decimal places
	{
		tc := tcTemp{
			info: "select floor(float64_col, 1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.12, 1.19, -1.12, -1.19}, []bool{false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{1}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.1, 1.1, -1.2, -1.2}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Floor with negative decimal places
	{
		tc := tcTemp{
			info: "select floor(float64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{14.0, 15.0, 19.0, -14.0, -19.0}, []bool{false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{10.0, 10.0, 10.0, -20.0, -20.0}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorFloat64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_CeilInt64 tests CEIL function for int64
func Test_CeilInt64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	{
		tc := tcTemp{
			info: "select ceil(int64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{14, 15, 19, -14, -19, 11, -11}, []bool{false, false, false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				// 14->20, 15->20, 19->20, -14->-10, -19->-10, 11->20, -11->-10
				[]int64{20, 20, 20, -10, -10, 20, -10}, []bool{false, false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CeilInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_FloorInt64 tests FLOOR function for int64
func Test_FloorInt64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	{
		tc := tcTemp{
			info: "select floor(int64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{14, 15, 19, -14, -19, 11, -11}, []bool{false, false, false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				// 14->10, 15->10, 19->10, -14->-20, -19->-20, 11->10, -11->-20
				[]int64{10, 10, 10, -20, -20, 10, -20}, []bool{false, false, false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Floor with precision 0
	{
		tc := tcTemp{
			info: "select floor(int64_col, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{123, -456, 0}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{123, -456, 0}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_FloorUint64 tests FLOOR function for uint64
func Test_FloorUint64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	{
		tc := tcTemp{
			info: "select floor(uint64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{14, 15, 19, 11, 99}, []bool{false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				// 14->10, 15->10, 19->10, 11->10, 99->90
				[]uint64{10, 10, 10, 10, 90}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorUInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}

	// Floor with precision -2
	{
		tc := tcTemp{
			info: "select floor(uint64_col, -2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{149, 150, 199, 250}, []bool{false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-2}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				// 149->100, 150->100, 199->100, 250->200
				[]uint64{100, 100, 100, 200}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, FloorUInt64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_CeilUint64 tests CEIL function for uint64
func Test_CeilUint64(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	{
		tc := tcTemp{
			info: "select ceil(uint64_col, -1)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{11, 14, 15, 19, 20}, []bool{false, false, false, false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{-1}, nil),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				// 11->20, 14->20, 15->20, 19->20, 20->20
				[]uint64{20, 20, 20, 20, 20}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CeilUint64)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, tc.info, info)
	}
}
